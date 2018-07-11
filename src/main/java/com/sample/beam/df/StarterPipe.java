/**
 * ===========================================
 * The code is for DEMO purpose only and it is 
 * not intended to be put in production
 * ===========================================
 * 
 */

package com.sample.beam.df;

import java.net.URL;

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
import com.sample.beam.df.process.CsvEmployeeProcess;
import com.sample.beam.df.shared.Employee;
import com.sample.beam.df.utils.DatabaseOptions;
import com.sample.beam.df.utils.Utils;

public class StarterPipe {
	private static final Logger LOG = LoggerFactory.getLogger(StarterPipe.class);
	private static final String DEFAULT_CONFIG_FILE = "/application1.properties";
//	private static Configuration config;
	private DatabaseOptions options;

	public static void main(String[] args) {

		StarterPipe sp = new StarterPipe();		
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
		PCollection<String> lines = pipeline.apply(TextIO.read().from("gs://ristemp/employee.csv"));
		PCollection<Employee> empRows=lines.apply("Convert to Employee",ParDo.of(new CsvEmployeeProcess()));

		//Convert messages into TableRow for BigQuery
		PCollection<TableRow> tableRowsToWrite=empRows.apply("Table row conversion",ParDo.of(new BigQueryEmployeeProcess()));		

		//Write into BigQuery
		tableRowsToWrite.apply("Write message into BigQuery",
				BigQueryIO.writeTableRows()
				.to("training-sandbox-sgp" + ":" + options.getBQDatasetId() + "." + options.getBQTableName())
				.withSchema(BigQueryEmployeeProcess.getSchema(Employee.getClassSchema()))
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
				);
	}

	public void init(String propFile)
	{
//		 URL propFileURL = this.getClass().getClassLoader().getResource(propFile);
//
//		Parameters params = new Parameters();
//		FileBasedConfigurationBuilder<FileBasedConfiguration> builder =
//				new FileBasedConfigurationBuilder<FileBasedConfiguration>(PropertiesConfiguration.class)
//				.configure(params.properties().setURL(propFileURL));
		
		try
		{
			LOG.info("Init Config start");
//			config = builder.getConfiguration();

			//define pipeline options
			options = PipelineOptionsFactory.create().as(DatabaseOptions.class);

			// Set DataFlow options
			options.setAppName("dataflow-app-test");
			options.setStagingLocation("gs://rislabs/iot-dataflow/staging");
			
			String tempLocation = "gs://rislabs/iot-dataflow/temp";
			
			LOG.info("Temp location:"+tempLocation);
			options.setTempLocation(tempLocation);
			
			options.setRunner(DataflowRunner.class);
//			options.setRunner(DirectRunner.class);
			options.setStreaming(false);
			options.setProject("training-sandbox-sgp");
			options.setAutoscalingAlgorithm(AutoscalingAlgorithmType.THROUGHPUT_BASED);
			options.setMaxNumWorkers(1);
			options.setJobName("testdf"+Utils.dateSecFormatter.format(new java.util.Date()));

			// Set BigQuery options
			options.setBQDatasetId("employee");
			options.setBQTableName("emp_details_data");
		}
		catch(Exception cex)
		{
			LOG.error("Exception during initialization of properties:",cex);
		}
	}

}
