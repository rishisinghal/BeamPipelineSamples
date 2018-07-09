/**
 * ===========================================
 * The code is for DEMO purpose only and it is 
 * not intended to be put in production
 * ===========================================
 * 
 */

package com.sample.beam.df;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.sample.beam.df.process.BigQueryEmployeeProcess;
import com.sample.beam.df.process.EmployeeMsg;
import com.sample.beam.df.shared.Employee;
import com.sample.beam.df.utils.DatabaseOptions;
import com.sample.beam.df.utils.Utils;

public class StarterPipelineCsvAvro {
	private static final Logger LOG = LoggerFactory.getLogger(StarterPipelineCsvAvro.class);
	private static final String DEFAULT_CONFIG_FILE = "application1.properties";
	private static Configuration config;
	private DatabaseOptions options;

	public static void main(String[] args) {

		StarterPipelineCsvAvro sp = new StarterPipelineCsvAvro();		
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
		PCollection<String> lines = pipeline.apply(TextIO.read().from(config.getString("csv.location")));
		PCollection<Employee> empRows=lines.apply("Convert to Employee",ParDo.of(new EmployeeMsg()));

		//Convert messages into TableRow for BigQuery
		PCollection<TableRow> tableRowsToWrite=empRows.apply("Table row conversion",ParDo.of(new BigQueryEmployeeProcess()));		

		//Write into BigQuery
		tableRowsToWrite.apply("Write message into BigQuery",
				BigQueryIO.writeTableRows()
				.to(config.getString("gcp.projectId") + ":" + options.getBQDatasetId() + "." + options.getBQTableName())
				.withSchema(BigQueryEmployeeProcess.getSchema(Employee.getClassSchema()))
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
				);
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
			
			options.setRunner(DataflowRunner.class);
//			options.setRunner(DirectRunner.class);
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
