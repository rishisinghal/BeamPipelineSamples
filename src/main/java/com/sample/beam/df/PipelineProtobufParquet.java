/**
 * ===========================================
 * The code is for DEMO purpose only and it is 
 * not intended to be put in production
 * ===========================================
 * 
 */

package com.sample.beam.df;

import java.io.IOException;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.protobuf.ProtobufDatumReader;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.GeneratedMessageV3;
import com.sample.beam.df.shared.EmpProtos.Emp;
import com.sample.beam.df.utils.DatabaseOptions;
import com.sample.beam.df.utils.Utils;

public class PipelineProtobufParquet {
	private static final Logger LOG = LoggerFactory.getLogger(PipelineProtobufParquet.class);
	private static final String DEFAULT_CONFIG_FILE = "application1.properties";
	private static Configuration config;
	private DatabaseOptions options;

	public static void main(String[] args) {

		PipelineProtobufParquet sp = new PipelineProtobufParquet();		
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
		PCollection<Metadata> matches = pipeline.apply(FileIO.match().filepattern(config.getString("protofile.location")));
		PCollection<FileIO.ReadableFile> decompressedAuto = matches.apply("Read File", 
				FileIO.readMatches().withCompression(Compression.AUTO));
		
		ProtobufDatumReader<Emp> datumReader = new ProtobufDatumReader<Emp>(Emp.class);
		Schema schema = datumReader.getSchema();
		PCollection<GenericRecord> empRows = decompressedAuto.apply("Convert to GenRecord",ParDo.of(new ReadableFileToGenRecordsFn(schema)));

		empRows.setCoder(AvroCoder.of(schema))
		.apply("Write to GCS in Parquet format",
				FileIO.<GenericRecord>write()
				.via(ParquetIO.sink(schema))
				.to(config.getString("parquet.location")));
	}

	public static class ReadableFileToGenRecordsFn extends DoFn<FileIO.ReadableFile, GenericRecord> {

		Schema schema;

		public ReadableFileToGenRecordsFn(Schema schema) {
			this.schema = schema;
		}

		@ProcessElement
		public void processElement(ProcessContext c) throws IOException {
			
			Emp e = Emp.parseFrom(c.element().readFullyAsBytes()); 
			Log.info("Start convert:"+e.toString());
			
			GenericRecordBuilder builder=new GenericRecordBuilder(schema);

			for(Map.Entry<FieldDescriptor,Object> entry : e.getAllFields().entrySet())
			{
//				Log.info("Field is:"+f.name());
				builder.set(entry.getKey().getName(), entry.getValue());
			}
			c.output(builder.build());
		}
	}

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
