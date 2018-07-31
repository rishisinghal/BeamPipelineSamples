/**
 * ===========================================
 * The code is for DEMO purpose only and it is 
 * not intended to be put in production
 * ===========================================
 * 
 */

package com.sample.beam.df;

import java.sql.ResultSet;
import java.util.ArrayList;

import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO.Read;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.sample.beam.df.utils.DatabaseOptions;
import com.sample.beam.df.utils.Utils;

public class StarterPipelineDb {
	private static final Logger LOG = LoggerFactory.getLogger(StarterPipelineDb.class);
	private static final String DEFAULT_CONFIG_FILE = "application1.properties";
	private static Configuration config;
	private DatabaseOptions options;

	public static void main(String[] args) {

		StarterPipelineDb sp = new StarterPipelineDb();		
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

	private static Read<TableRow> readDBRows() {

		return JdbcIO.<TableRow>read()
				.withDataSourceConfiguration(
						JdbcIO.DataSourceConfiguration.create(config.getString("jdbc.driver"),config.getString("jdbc.url"))
						.withUsername(config.getString("jdbc.user")))
				.withQuery(config.getString("jdbc.query"))						
				.withCoder(TableRowJsonCoder.of())
				.withRowMapper(new JdbcIO.RowMapper<TableRow>() {
					private static final long serialVersionUID = 1L;

					public TableRow mapRow(ResultSet resultSet) throws Exception {

						TableRow r = new TableRow();
						r.set("empId", resultSet.getInt(1));
						r.set("name", resultSet.getString(2));
						r.set("deptno", resultSet.getString(3));
						r.set("joindate", resultSet.getDate(4));

						return r;
					}
				});
	}	

	public void doDataProcessing(Pipeline pipeline)
	{
		PCollection<TableRow> tableRows = pipeline.apply(readDBRows());

		ArrayList<TableFieldSchema> fieldSchema = new ArrayList<TableFieldSchema>();
		fieldSchema.add(new TableFieldSchema().setName("empId").setType("INTEGER"));
		fieldSchema.add(new TableFieldSchema().setName("name").setType("STRING"));
		fieldSchema.add(new TableFieldSchema().setName("deptno").setType("STRING").setMode("NULLABLE"));
		fieldSchema.add(new TableFieldSchema().setName("joindate").setType("DATE").setMode("NULLABLE"));

		TableSchema schema = new TableSchema();
		schema.setFields(fieldSchema);

		//Write into BigQuery
		tableRows.apply("Write message into BigQuery",
				BigQueryIO.writeTableRows()
				.to(config.getString("gcp.projectId") + ":" + options.getBQDatasetId() + "." + options.getBQTableName())
				.withSchema(schema)
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
			options.setTempLocation(config.getString("gcs.urlBase") + config.getString("gcs.bucketName") + 
					"/"+config.getString("gcs.tempLocation"));
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
