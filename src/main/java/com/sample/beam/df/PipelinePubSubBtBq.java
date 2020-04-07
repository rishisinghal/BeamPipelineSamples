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
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy.Context;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
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
import com.google.bigtable.v2.Mutation;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.protobuf.ByteString;
import com.sample.beam.df.process.BigQueryTelemetryProcess;
import com.sample.beam.df.process.BigTableTelemetryProcess;
import com.sample.beam.df.process.IoTTelemetryMsg;
import com.sample.beam.df.shared.DeviceTelemetry;
import com.sample.beam.df.utils.TowerOptions;
import com.sample.beam.df.utils.Utils;

public class PipelinePubSubBtBq {
	private static final Logger LOG = LoggerFactory.getLogger(PipelinePubSubBtBq.class);
	private static final String DEFAULT_CONFIG_FILE = "application.properties";
	private Configuration config;
	private TowerOptions options;

	public static void main(String[] args) {

		PipelinePubSubBtBq sp = new PipelinePubSubBtBq();		
		String propFile = null;

		if(args.length > 0) // For custom properties file read from cmd line
			propFile = args[0];
		else
			propFile = DEFAULT_CONFIG_FILE; // Default application.properties file

		sp.init(propFile);
		sp.run();		
	}

	public void run()
	{
		// Create BigTable table if it does not exist
		BigtableOptions.Builder optionsBuilder = new BigtableOptions.Builder()
				.setProjectId(options.getProject()) 
				.setInstanceId(config.getString("bt.instanceId"))
				.setUserAgent(config.getString("bt.userAgent"));

		BigTableTelemetryProcess btTeleObj = new BigTableTelemetryProcess(options.getBTColFamily());
		btTeleObj.createEmptyTable(optionsBuilder, options);	

		//create Pipeline with options
		Pipeline pipeline = Pipeline.create(options);
		PCollection<DeviceTelemetry> deviceDataRows = doTelemetryProcessing(optionsBuilder, pipeline, btTeleObj);
		
		//Convert messages into TableRow for BigQuery
		PCollection<TableRow> tableRowsToWrite=deviceDataRows.apply("Table row conversion",ParDo.of(new BigQueryTelemetryProcess()));		
		
		// create mutations for BigTable
		PCollection<KV<ByteString, Iterable<Mutation>>> mutations = deviceDataRows.apply("Table row conversion",ParDo.of(btTeleObj));

		//Write into BigQuery
		 WriteResult writeResult = tableRowsToWrite.apply("Write message into BigQuery",
				BigQueryIO.writeTableRows()
				.to(config.getString("gcp.projectId") + ":" + options.getBQDatasetId() + "." + options.getBQTableName())
				.withSchema(BigQueryTelemetryProcess.getTableSchema(DeviceTelemetry.getClassSchema()))
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
				);
		 
		//Write into BigTable	
		mutations.apply("Write telemetry message to BigTable",
				BigtableIO.write()
				.withBigtableOptions(optionsBuilder.build())
				.withTableId(options.getBTTelemetryTableId()));
		
		pipeline.run();
	}
	
	public PCollection<DeviceTelemetry> doTelemetryProcessing(BigtableOptions.Builder optionsBuilder, Pipeline pipeline,
															  BigTableTelemetryProcess btTeleObj)
	{
		//read messages from Pub/Sub 
		PCollection<String> pubSubTelemetryMsg=pipeline.apply("Read telemetry msg from PubSub",
				PubsubIO.readStrings().fromSubscription(config.getString("pubsub.subscription.telemetry")));
		// convert to DeviceMessage
		PCollection<DeviceTelemetry>  devTeleMsgRows=pubSubTelemetryMsg.apply("Convert to DeviceMessage",ParDo.of(new IoTTelemetryMsg()));
		return devTeleMsgRows;
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
			options = PipelineOptionsFactory.create().as(TowerOptions.class);

			// Set DataFlow options
			options.setAppName(config.getString("df.appName"));
			options.setStagingLocation(config.getString("gcs.urlBase") + config.getString("gcs.bucketName") + 
					"/"+config.getString("gcs.stagingLocation"));
			options.setTempLocation(config.getString("gcs.urlBase") + config.getString("gcs.bucketName") + 
					"/"+config.getString("gcs.tempLocation"));
			options.setRunner(DataflowRunner.class);
//			options.setRunner(DirectRunner.class);
			options.setStreaming(true);
			options.setProject(config.getString("gcp.projectId"));
			options.setAutoscalingAlgorithm(AutoscalingAlgorithmType.THROUGHPUT_BASED);
			options.setMaxNumWorkers(config.getInt("df.maxWorkers"));
			options.setJobName(config.getString("df.baseJobName")+Utils.dateSecFormatter.format(new java.util.Date()));

			// Set BigQuery options
			options.setBQDatasetId(config.getString("bq.datasetId"));
			options.setBQTableName(config.getString("bq.telemetryTable"));

			// Set BigTable options
			options.setBTInstanceId(String.format("projects/%s/instances/%s", config.getString("gcp.projectId"), 
					config.getString("bt.instanceId")));
			options.setBTTelemetryTableId(config.getString("bt.telemetryTable"));
			options.setBTColFamily(config.getString("bt.columnFamily"));
			options.setWorkerDiskType("compute.googleapis.com/projects//zones//diskTypes/pd-ssd");

		}
		catch(ConfigurationException cex)
		{
			LOG.error("Exception during initialization of properties:",cex);
		}
	}

}
