/**
 * ===========================================
 * The code is for DEMO purpose only and it is
 * not intended to be put in production
 * ===========================================
 *
 */

package com.sample.beam.df;

import com.sample.beam.df.utils.DatabaseOptions;
import com.sample.beam.df.utils.Utils;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineStreamPubSubtoBQSchema {
	private static final Logger LOG = LoggerFactory.getLogger(PipelinePubsubSpanner.class);
	private static final String DEFAULT_CONFIG_FILE = "application1.properties";
	private static Configuration config;
	private DatabaseOptions options;

	public static void main(String[] args) {

		PipelineStreamPubSubtoBQSchema sp = new PipelineStreamPubSubtoBQSchema();
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
		//read messages from Pub/Sub
		PCollection<String> lines=pipeline.apply("Read msg from PubSub",
				PubsubIO.readStrings().fromSubscription(config.getString("pubsub.subscription"))
		);

//		lines.apply("Print messages", ParDo.of(new PrintProcess()));

		lines.apply(Window.<String>into(FixedWindows.of(Duration.standardMinutes(1)))
				.triggering(AfterWatermark.pastEndOfWindow()
						.withLateFirings(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.ZERO)))
				.withAllowedLateness(Duration.standardMinutes(2))
				.discardingFiredPanes())
				.apply("Write to GCS", TextIO.write().to(config.getString("gcs.writeFolderPath"))
						.withWindowedWrites()
						.withNumShards(1)
						.withCompression(Compression.GZIP));
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
			options.setTempLocation(config.getString("gcs.urlBase") + config.getString("gcs.bucketName") +
					"/"+config.getString("gcs.tempLocation"));
			options.setRunner(DataflowRunner.class);
//			options.setRunner(DirectRunner.class);
			options.setRegion(config.getString("df.region"));
			options.setStreaming(true);
			options.setProject(config.getString("gcp.projectId"));
			options.setAutoscalingAlgorithm(AutoscalingAlgorithmType.THROUGHPUT_BASED);
			options.setMaxNumWorkers(config.getInt("df.maxWorkers"));
			options.setJobName(config.getString("df.baseJobName")+Utils.dateSecFormatter.format(new java.util.Date()));
		}
		catch(ConfigurationException cex)
		{
			LOG.error("Exception during initialization of properties:",cex);
		}
	}

}
