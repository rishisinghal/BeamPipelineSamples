/**
 * ===========================================
 * The code is for DEMO purpose only and it is 
 * not intended to be put in production
 * ===========================================
 * 
 */

package com.sample.beam.df;

import static org.apache.commons.lang3.CharEncoding.UTF_8;

import java.io.UnsupportedEncodingException;
import java.sql.ResultSet;

import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO.Read;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.ToString;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.sample.beam.df.utils.DatabaseOptions;
import com.sample.beam.df.utils.Utils;

public class StarterPipelineCsvSideInput {
	private static final Logger LOG = LoggerFactory.getLogger(StarterPipelineCsvSideInput.class);
	private static final String DEFAULT_CONFIG_FILE = "application1.properties";
	private static Configuration config;
	private DatabaseOptions options;

	public static void main(String[] args) {

		StarterPipelineCsvSideInput sp = new StarterPipelineCsvSideInput();		
		String propFile = null;

		if(args.length > 0) // For custom properties file
			propFile = args[0];
		else
			propFile = DEFAULT_CONFIG_FILE;

		sp.init(propFile);
		sp.run();		
	}

	public void run()
	{
		//create Pipeline with options
		Pipeline pipeline = Pipeline.create(options);
		doDataProcessing(pipeline);
		pipeline.run();

	}

	public void doDataProcessing(Pipeline pipeline)
	{
		Storage storage = StorageOptions.getDefaultInstance().getService();
		Blob blob = storage.get("ristemp", "employee.csv");
		String fileContent = new String(blob.getContent());
		System.out.println("Content is:"+fileContent);

		PCollection<String> emptyLines = pipeline.apply(TextIO.read().from("gs://ristemp/employee1.csv"));
		PCollection<String> fullLines = pipeline.apply(TextIO.read().from("gs://ristemp/employee.csv"));

		// Get number of characters in each line
		PCollection<Integer> emptyNoChars = emptyLines.apply(ParDo.of(new LineProcess()));
		PCollection<Integer> linesNoChars = fullLines.apply(ParDo.of(new LineProcess()));

		// Find the total number of characters in the entire file.
		PCollection<Integer> totalChars = emptyNoChars.apply(Combine.globally(new SumCounts()));
		final PCollectionView<Integer> countSideInput1 = totalChars.apply(View.asSingleton());	

		PCollection<Integer> totalFullChars = linesNoChars.apply(Combine.globally(new SumCounts()));

		// We don't want to write to the file if the PCollection lines is empty. We don't have 
		// a straightforward way here to check isEmpty so here's the trick
		PCollection<Integer>  maxCount = totalFullChars.apply(ParDo.of(new WriteNonEmptyProcess1(countSideInput1)).withSideInputs(countSideInput1));
		maxCount.apply("Convert to string", ToString.elements()).apply("Write to file",
				TextIO.write().to("gs://ristemp/counttest.txt").withoutSharding());	
	}

	public static class WriteNonEmptyProcess1 extends DoFn<Integer, Integer> {

		private static final long serialVersionUID = 1462827258689031685L;
		private final PCollectionView<Integer> view;

		public WriteNonEmptyProcess1(PCollectionView<Integer> view) {
			this.view = view;
		}

		@ProcessElement
		public void processElement(DoFn<Integer, Integer>.ProcessContext c) throws Exception {

			int sideInput = c.sideInput(view);
			int numOfChars = c.element();
			System.out.println("number is:"+numOfChars);
			if(numOfChars > sideInput)
			{
				c.output(numOfChars);
			} else
				c.output(sideInput);
		}
	}

	public static class LineProcess extends DoFn<String, Integer> {

		private static final long serialVersionUID = 1L;

		@ProcessElement
		public void processElement(ProcessContext c) throws Exception {
			c.output(c.element().length());
		}
	}

	public static class SumCounts implements SerializableFunction<Iterable<Integer>, Integer> {

		private static final long serialVersionUID = 1L;

		@Override
		public Integer apply(Iterable<Integer> input) {
			int sum = 0;
			for (Integer item : input) {
				sum += item;
			}
			return sum;
		}
	}

	//	public static class WriteNonEmptyProcess extends DoFn<Integer, Void> {
	//
	//		private static final long serialVersionUID = 1462827258689031685L;
	//		private final PCollectionView<Long> view;
	//		
	//		public WriteNonEmptyProcess(PCollectionView<Long> view) {
	//	        this.view = view;
	//	      }
	//
	//		@ProcessElement
	//		public void processElement(DoFn<Integer, Void>.ProcessContext c) throws Exception {
	//
	//			long empty = c.sideInput(view);
	//			Integer numOfChars = c.element();
	//			System.out.println("number is:"+numOfChars);
	//			if(empty > 0)
	//			{
	//				// write to GCS
	//				System.out.println("Writing to GCS");
	//				Storage storage = StorageOptions.getDefaultInstance().getService();
	//
	//				String bucketName = "ristemp";
	//				String blobName = "counttest.txt";
	//				BlobId blobId = BlobId.of(bucketName, blobName);
	//
	//				BlobInfo blobInfo = BlobInfo.newBuilder(blobId)
	//						.setContentType("text/plain")
	//						.build();
	//				try {
	//					Blob blob = storage.create(blobInfo, numOfChars.toString().getBytes(UTF_8));
	//					System.out.println("Write complete");
	//				} catch (UnsupportedEncodingException e) {
	//					e.printStackTrace();
	//				}
	//			} else
	//				System.out.println("No write to GCS");
	//		}
	//	}

	public void init(String propFile)
	{
		Parameters params = new Parameters();
		FileBasedConfigurationBuilder<FileBasedConfiguration> builder =
				new FileBasedConfigurationBuilder<FileBasedConfiguration>(PropertiesConfiguration.class)
				.configure(params.properties()
						.setFileName(propFile));
		try
		{
			LOG.info("Init Config start");
			config = builder.getConfiguration();

			//define pipeline options
			options = PipelineOptionsFactory.create().as(DatabaseOptions.class);

			// Set DataFlow options
			options.setAppName(config.getString("df.appName"));
			options.setStagingLocation(config.getString("gcs.urlBase") + config.getString("gcs.bucketName") + 
					"/"+config.getString("gcs.stagingLocation"));

			String tempLocation = config.getString("gcs.urlBase") + config.getString("gcs.bucketName") + 
					"/"+config.getString("gcs.tempLocation");

			LOG.info("Temp location:"+tempLocation);
			options.setTempLocation(tempLocation);

			//			options.setRunner(DataflowRunner.class);
			options.setRunner(DirectRunner.class);
			options.setStreaming(false);
			options.setProject(config.getString("gcp.projectId"));
			options.setAutoscalingAlgorithm(AutoscalingAlgorithmType.THROUGHPUT_BASED);
			options.setMaxNumWorkers(config.getInt("df.maxWorkers"));
			options.setJobName(config.getString("df.baseJobName")+Utils.dateSecFormatter.format(new java.util.Date()));

			// Set BigQuery options
			options.setBQDatasetId(config.getString("bq.datasetId"));
			options.setBQTableName(config.getString("bq.empTable"));
		}
		catch(ConfigurationException cex)
		{
			LOG.error("Exception during initialization of properties:",cex);
		}
	}

}
