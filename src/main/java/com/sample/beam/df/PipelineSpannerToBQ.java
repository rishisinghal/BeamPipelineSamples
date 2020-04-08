package com.sample.beam.df;

import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Map;

import com.google.cloud.spanner.Struct;


import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sample;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.spanner.Mutation;
import com.sample.beam.df.process.CsvEmployeeProcess;
import com.sample.beam.df.process.SpannerDeptMsg;
import com.sample.beam.df.process.SpannerEmployeeMsg;
import com.sample.beam.df.shared.Employee;
import com.sample.beam.df.utils.DatabaseOptions;
import com.sample.beam.df.utils.SpannerToBQOptions;
import com.sample.beam.df.utils.StructToTableRowConverter;
import com.sample.beam.df.utils.Utils;
import org.apache.beam.sdk.transforms.View;


public class PipelineSpannerToBQ {
	private static final Logger LOG = LoggerFactory.getLogger(PipelineSpannerToBQ.class);
	private static final String DEFAULT_CONFIG_FILE = "application1.properties";
	private static Configuration config;
	private SpannerToBQOptions options;
	private static final String NULL_DATE = "1950-01-01";

	public static void main(String[] args) {

		PipelineSpannerToBQ sp = new PipelineSpannerToBQ();		
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
		 final String bqTarget = options.getProject()+":"+options.getBQDatasetId()+"."+options.getBQTableName();
		 PCollection<Struct> rows = pipeline.apply(
			        SpannerIO.read()
			            .withInstanceId(config.getString("spanner.instanceId"))
			            .withDatabaseId(config.getString("spanner.databaseId"))
			            .withTable(config.getString("spanner.table"))			         
			            .withColumns(Arrays.asList(config.getString("spanner.columns").split(","))));

			    final PCollectionView<Map<String,String>> schemaView = rows
			        .apply("SampleStruct", Sample.any(1))
			        .apply("AsMap", MapElements
			            .into(TypeDescriptors.maps(TypeDescriptors.strings(),TypeDescriptors.strings()))
			            .via(struct -> StructToTableRowConverter.convertSchema(bqTarget, struct)))
			        .apply("AsView", View.asSingleton());

			    rows.apply(
			        "Write to BigQuery",
			        BigQueryIO.<Struct>write()
					.to(bqTarget)
			            .withFormatFunction(StructToTableRowConverter::convert)
			            .withSchemaFromView(schemaView)
			            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
			            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
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
			options = PipelineOptionsFactory.create().as(SpannerToBQOptions.class);

			// Set DataFlow options
			options.setAppName(config.getString("df.appName"));
			options.setStagingLocation(config.getString("gcs.urlBase") + config.getString("gcs.bucketName") + 
					"/"+config.getString("gcs.stagingLocation"));
			options.setTempLocation(config.getString("gcs.urlBase") + config.getString("gcs.bucketName") + 
					"/"+config.getString("gcs.tempLocation"));
			options.setRunner(DataflowRunner.class);
//			options.setRunner(DirectRunner.class);
			options.setStreaming(false);
			options.setProject(config.getString("gcp.projectId"));
			options.setAutoscalingAlgorithm(AutoscalingAlgorithmType.THROUGHPUT_BASED);
			options.setMaxNumWorkers(config.getInt("df.maxWorkers"));
			options.setJobName(config.getString("df.baseJobName")+Utils.dateSecFormatter.format(new java.util.Date()));
			
			options.setSpannerInstanceId(config.getString("spanner.instanceId"));
			options.setSpannerDatabaseId(config.getString("spanner.databaseId"));
			options.setSpannerTable(config.getString("spanner.table"));
			options.setSpannerColumns(config.getString("spanner.columns"));
			
			options.setBQDatasetId(config.getString("bq.datasetId"));
			options.setBQTableName(config.getString("bq.empTable"));
		}
		catch(ConfigurationException cex)
		{
			LOG.error("Exception during initialization of properties:",cex);
		}
	}

}
