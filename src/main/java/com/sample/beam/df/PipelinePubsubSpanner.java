/**
 * ===========================================
 * The code is for DEMO purpose only and it is 
 * not intended to be put in production
 * ===========================================
 * 
 */

package com.sample.beam.df;

import java.util.concurrent.ThreadLocalRandom;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.spanner.Mutation;
import com.sample.beam.df.utils.DatabaseOptions;
import com.sample.beam.df.utils.Utils;

public class PipelinePubsubSpanner {
	private static final Logger LOG = LoggerFactory.getLogger(PipelinePubsubSpanner.class);
	private static final String DEFAULT_CONFIG_FILE = "application1.properties";
	private static Configuration config;
	private DatabaseOptions options;

	public static void main(String[] args) {

		PipelinePubsubSpanner sp = new PipelinePubsubSpanner();		
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
				PubsubIO.readStrings().fromSubscription(config.getString("pubsub.subscription")));

		PCollection<KV<String, Integer>> lKey=lines.apply(ParDo.of(new LinesKey()));

		PCollection<KV<String, Integer>> fixedWindowedItems = lKey.apply(
				Window.<KV<String, Integer>>into(FixedWindows.of(Duration.standardMinutes(1)))
				.withAllowedLateness(Duration.standardDays(1))
				.accumulatingFiredPanes())
				.apply(new TotalFlow());

		PCollection<Mutation> empMut = fixedWindowedItems.apply("EmpBadge Mutation", ParDo.of(new FormatTotalFlow()));

		empMut.apply(Window.<Mutation>into(new GlobalWindows())
			    .triggering(AfterWatermark.pastEndOfWindow().withEarlyFirings(AfterPane.elementCountAtLeast(1)))
			    .discardingFiredPanes());
		
		// Finally write the Mutations to Spanner
		empMut.apply("Write Employee Badge data", SpannerIO.write()
				.withInstanceId(config.getString("spanner.instanceId"))
				.withDatabaseId(config.getString("spanner.databaseId")));
	}

	static class LinesKey extends DoFn<String, KV<String, Integer>> {

		@ProcessElement
		public void processElement(DoFn<String, KV<String, Integer>>.ProcessContext c) throws Exception {

			try {
				LOG.info("processing :"+c.element());
				c.output(KV.of(c.element(),1));
//				c.outputWithTimestamp(KV.of(c.element(),1),new Instant());
			} catch(Exception e)
			{
				LOG.error("Exception in processing :"+e.getMessage(), e);
			}
		}
	}

	static class TotalFlow extends PTransform<PCollection<KV<String, Integer>>, PCollection<KV<String, Integer>>> {

		@Override
		public PCollection<KV<String, Integer>> expand(PCollection<KV<String, Integer>> flowInfo) {
			LOG.info("In expand");

			PCollection<KV<String, Iterable<Integer>>> flowPerFreeway = flowInfo.apply(GroupByKey.create());

			PCollection<KV<String, Integer>> results =
					flowPerFreeway.apply(
							ParDo.of(
									new DoFn<KV<String, Iterable<Integer>>, KV<String, Integer>>() {

										@ProcessElement
										public void processElement(ProcessContext c) throws Exception {
											Iterable<Integer> flows = c.element().getValue();
											Integer numberOfRecords = 0;
											for (Integer value : flows) {
												numberOfRecords++;
											}
											LOG.info("Aggregate :"+c.element().getKey()+"::"+numberOfRecords);
											c.output(KV.of(c.element().getKey(),numberOfRecords));
										}
									}));
			return results;
		}
	}

	static class FormatTotalFlow extends DoFn<KV<String, Integer>, Mutation> {

		@ProcessElement
		public void processElement(ProcessContext c) throws Exception {

			LOG.info("Writing:"+c.element().getKey()+":"+c.element().getValue());
			c.output(Mutation.newInsertOrUpdateBuilder("empbadge")
					.set("emp_id").to(Integer.valueOf(c.element().getKey()))
					.set("badgein").to(c.element().getValue())
					.build());
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
			options.setTempLocation(config.getString("gcs.urlBase") + config.getString("gcs.bucketName") + 
					"/"+config.getString("gcs.tempLocation"));
//			options.setRunner(DataflowRunner.class);
			options.setRunner(DirectRunner.class);
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
