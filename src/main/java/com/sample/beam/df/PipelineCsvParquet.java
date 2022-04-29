/**
 * ===========================================
 * The code is for DEMO purpose only and it is
 * not intended to be put in production
 * ===========================================
 *
 */

package com.sample.beam.df;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
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
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.LocalDate;
import org.joda.time.MutableDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.beam.sdk.io.parquet.ParquetIO;

import com.sample.beam.df.process.CsvEmployeeProcess;
import com.sample.beam.df.shared.Employee;
import com.sample.beam.df.utils.DatabaseOptions;
import com.sample.beam.df.utils.Utils;

public class PipelineCsvParquet {
	private static final Logger LOG = LoggerFactory.getLogger(PipelineCsvParquet.class);
	private static final String DEFAULT_CONFIG_FILE = "application1.properties";
	private static Configuration config;
	private DatabaseOptions options;

	public static void main(String[] args) {

		PipelineCsvParquet sp = new PipelineCsvParquet();
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
		PCollection<Employee> empRows=lines.apply("Convert to Employee",ParDo.of(new CsvEmployeeProcess()));
		PCollection<GenericRecord> empRows1 = empRows.apply("Produce Avro records", ParDo.of(new EmployeeToGenericRecordFn()));

		empRows1.setCoder(AvroCoder.of(Employee.getClassSchema()))
		.apply("Write to GCS in Parquet format",
				FileIO.<GenericRecord>write()
				.via(ParquetIO.sink(Employee.getClassSchema()))
				.to(config.getString("parquet.location")));
	}

	public static class EmployeeToGenericRecordFn extends DoFn<Employee, GenericRecord> {
		@ProcessElement
		public void processElement(ProcessContext c) {
			LOG.info("Start convert:"+c.element().toString());

			Schema schema = Employee.getClassSchema();
			GenericRecordBuilder builder=new GenericRecordBuilder(schema);
			Employee e = c.element();

			for(Field f : schema.getFields())
			{
//				Log.info("Field is:"+f.name());
				if(e.get(f.name()) instanceof org.joda.time.LocalDate)
				{
					DateTime dt = ((LocalDate)e.get(f.name())).toDateTimeAtCurrentTime(DateTimeZone.UTC);
					MutableDateTime epoch = new MutableDateTime(0l, DateTimeZone.UTC);
					Days days = Days.daysBetween(epoch, dt);
					builder.set(f.name(), days.getDays());
				}
				else
					builder.set(f.name(), e.get(f.name()));
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
