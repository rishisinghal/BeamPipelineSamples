/**
 * ===========================================
 * The code is for DEMO purpose only and it is
 * not intended to be put in production
 * ===========================================
 *
 */

package com.sample.beam.df;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.sample.beam.df.process.*;
import com.sample.beam.df.utils.*;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigIntegerCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.gcp.bigquery.*;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.*;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

public class PipelineStreamPubSubtoBQDynamicSchema {
	private static final Logger LOG = LoggerFactory.getLogger(PipelinePubsubSpanner.class);
	private static final String DEFAULT_CONFIG_FILE = "application1.properties";
	private static Configuration config;
	private static DatabaseOptions options;

	public static void main(String[] args) {

		PipelineStreamPubSubtoBQDynamicSchema sp = new PipelineStreamPubSubtoBQDynamicSchema();
		String propFile = null;

		if(args.length > 0) // For custom properties file
			propFile = args[0];
		else
			propFile = DEFAULT_CONFIG_FILE;

		sp.init(propFile);
		sp.run();
	}

	public static void run()
	{
		//create Pipeline with options
		Pipeline pipeline = Pipeline.create(options);
		doDataProcessing(pipeline);
		pipeline.run();
	}

	public static void doDataProcessing(Pipeline pipeline)
	{
		TableRowWithSchemaCoder.registerCoder(pipeline);
		CustomDestinationCoder.registerCoder(pipeline);

		Map<String, Object> kafkaConsumerProperties = new HashMap<>();
		kafkaConsumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "EventFlow_Consumer");
		kafkaConsumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//		PCollection<String> lines = pipeline.apply(
//				"Read from Kafka",
//				KafkaIO.<String, String>read()
//						.withBootstrapServers("10.128.0.49:9092")
//						.withTopic("pump-topic")
//						.withKeyDeserializer(StringDeserializer.class)
//						.withValueDeserializer(StringDeserializer.class)
//						.commitOffsetsInFinalize()
//						.withConsumerConfigUpdates(kafkaConsumerProperties)
//						.withoutMetadata())
//				.apply(Values.create());

		//read messages from Pub/Sub
		PCollection<String> lines=pipeline.apply("Read msg from PubSub",
				PubsubIO.readStrings().fromSubscription(config.getString("pubsub.subscription")));

		PCollection<String> jsonColl =  lines.apply("JSONify data", ParDo.of(new DynamicJsonProcess()));
		PCollection<TableRowWithSchema> tableRowWithSchemaPCollection = jsonColl.apply(ParDo.of(
				new JSONObjectToTableRow()));

		WriteResult writeResult = tableRowWithSchemaPCollection
				.apply("Insert BQ", BigQueryIO.<TableRowWithSchema>write()
						.to(new DynamicDestinations<TableRowWithSchema, TableRowWithSchema>() {

							@Override
							public TableRowWithSchema getDestination(ValueInSingleWindow<TableRowWithSchema> element) {
								TableRowWithSchema tableRowWithSchema = element.getValue();
								return tableRowWithSchema;
							}

							@Override
							public TableDestination getTable(TableRowWithSchema destination) {
								return new TableDestination(
										new TableReference()
												.setProjectId(destination.getTableMetaData().getProject())
												.setDatasetId(destination.getTableMetaData().getDataset())
												.setTableId(destination.getTableMetaData().getTable()), null);
							}

							@Override
							public TableSchema getSchema(TableRowWithSchema destination) {
								LOG.info("BQ write schema:"+destination.getTableSchema().toString());
								return destination.getTableSchema();
							}
						})
						.withExtendedErrorInfo()
						.withFormatFunction(new SerializableFunction<TableRowWithSchema, TableRow>() {
							@Override
							public TableRow apply(TableRowWithSchema tableRowWithSchema) {
								return tableRowWithSchema.getTableRow();
							}
						})
						.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
						.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
						.withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
						.withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors()));

		PCollection<String> failedRecordsColl = writeResult.getFailedInsertsWithErr()
				.apply(Window.<BigQueryInsertError>into(FixedWindows.of(Duration.standardMinutes(1)))
								.triggering(AfterWatermark.pastEndOfWindow()
										.withLateFirings(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.ZERO)))
								.withAllowedLateness(Duration.standardMinutes(2))
								.discardingFiredPanes())
				.apply("MutateSchema", BigQuerySchemaMutator.mutateWithSchema(
						config.getString("gcs.writeFolderPath")))
				.apply("Print messages", ParDo.of(new PrintProcess()));

		failedRecordsColl.apply("Write to Pub/Sub and retry",
				PubsubIO.writeStrings().to(config.getString("pubsub.topic")));

		PCollection<KV<BigInteger, TableRowWithSchema>> incomingRecordsColl = tableRowWithSchemaPCollection
				.apply("KeyIncomingByHash",WithKeys.of(record -> BigInteger.valueOf(record.getTableRow().hashCode())))
				.setCoder(KvCoder.of(BigIntegerCoder.of(), TableRowWithSchemaCoder.of()));

		PCollection<KV<BigInteger, TableRowWithSchema>> failedRowsColl = writeResult.getFailedInsertsWithErr()
				.apply(ParDo.of(new AllFailedInsertToBQSchema("myschema",
						"portfolioKey","", "timestamp",
						"event","events")));

		PCollectionList<KV<BigInteger, TableRowWithSchema>> combineCollections = PCollectionList
				.of(incomingRecordsColl).and(failedRowsColl);
		PCollection<KV<BigInteger, Iterable<TableRowWithSchema>>> groupedCollection = combineCollections
				.apply(Flatten.pCollections())
				.apply(Window.<KV<BigInteger, TableRowWithSchema>>into(FixedWindows.of(Duration.standardMinutes(1)))
						.triggering(AfterWatermark.pastEndOfWindow()
								.withLateFirings(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.ZERO)))
						.withAllowedLateness(Duration.standardMinutes(2))
						.discardingFiredPanes())
				.apply(GroupByKey.create());

		PCollection<String> successfulBQIngestions = groupedCollection.apply(
				ParDo.of(new SuccessfullyInsertedToString()));

		successfulBQIngestions.apply("Write successful ingestions to Pub/Sub",
				PubsubIO.writeStrings().to(config.getString("pubsub.topic1")));

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
			options.setNetwork("default");
			options.setSubnetwork("https://www.googleapis.com/compute/v1/projects/training-sandbox-sgp/regions/us-central1/subnetworks/default");
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
