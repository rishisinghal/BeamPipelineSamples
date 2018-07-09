/**
 * ===========================================
 * The code is for DEMO purpose only and it is 
 * not intended to be put in production
 * ===========================================
 * 
 */

package com.sample.beam.df;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.DoFn;
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

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.sample.beam.df.utils.DatabaseOptions;
import com.sample.beam.df.utils.Utils;

public class StarterPipelineDbNestedBQ {
	private static final Logger LOG = LoggerFactory.getLogger(StarterPipelineDbNestedBQ.class);
	private static final String DEFAULT_CONFIG_FILE = "application1.properties";
	private static Configuration config;
	private DatabaseOptions options;

	public static void main(String[] args) {

		StarterPipelineDbNestedBQ sp = new StarterPipelineDbNestedBQ();		
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

	private static org.apache.beam.sdk.io.jdbc.JdbcIO.Read<KV<Integer, TableRow>> readDBRows() {

		return JdbcIO.<KV<Integer, TableRow>>read()
				.withDataSourceConfiguration(
						JdbcIO.DataSourceConfiguration.create(config.getString("jdbc.driver"),config.getString("jdbc.url"))
						.withUsername(config.getString("jdbc.user")))
				.withQuery(config.getString("jdbc.query"))							
				.withCoder(KvCoder.of(BigEndianIntegerCoder.of(), TableRowJsonCoder.of()))
				.withRowMapper(new JdbcIO.RowMapper<KV<Integer, TableRow>>() {
					private static final long serialVersionUID = 1L;

					public KV<Integer, TableRow> mapRow(ResultSet resultSet) throws Exception {

						int key = resultSet.getInt(1);

						TableRow r = new TableRow();
						r.set("empId", resultSet.getInt(1));
						r.set("name", resultSet.getString(2));
						r.set("deptno", resultSet.getString(3));
						r.set("joindate", resultSet.getDate(4));

						return KV.of(key, r);
					}
				});
	}	

	static class ComputeRowFn extends DoFn<KV<Integer, TableRow>, TableRow> {
		@ProcessElement
		public void processElement(ProcessContext c) {
			c.output(c.element().getValue());
		}
	}

	public void doDataProcessing(Pipeline pipeline)
	{

		PCollection<KV<Integer, TableRow>> empMapColl = pipeline.apply(readDBRows());
		PCollection<KV<Integer, TableRow>> groupedEmpDateColl = empMapColl.apply(Combine.perKey(new MergeDept()));
		PCollection<TableRow> tableRows = groupedEmpDateColl.apply(ParDo.of(new ComputeRowFn()));

		ArrayList<TableFieldSchema> fieldSchema = new ArrayList<TableFieldSchema>();
		fieldSchema.add(new TableFieldSchema().setName("empId").setType("INTEGER"));
		fieldSchema.add(new TableFieldSchema().setName("name").setType("STRING"));

		fieldSchema.add(
				new TableFieldSchema().setName("dept").setType("RECORD").setMode("REPEATED").setFields(new ArrayList<TableFieldSchema>() {
					{
						add(new TableFieldSchema().setName("deptno").setType("STRING").setMode("NULLABLE"));
						add(new TableFieldSchema().setName("joindate").setType("DATE").setMode("NULLABLE"));
					}
				}));

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

	public static class MergeDept extends CombineFn<TableRow,  MergeDept.Accum, TableRow>  {

		public static class Accum {
			List<TableRow> empList;

			public Accum() {
				empList = new ArrayList<TableRow>();
			}

			public Accum(List<TableRow> empList) {
				this.empList=empList;
			}

			public static Coder<Accum> getCoder() {
				return new AtomicCoder<Accum>() {

					@Override
					public void encode(Accum value, OutputStream outStream) throws CoderException, IOException {
						ListCoder.of(TableRowJsonCoder.of()).encode(value.empList, outStream);					
					}

					@Override
					public Accum decode(InputStream inStream) throws CoderException, IOException {
						List<TableRow> empList = ListCoder.of(TableRowJsonCoder.of()).decode(inStream);						
						return new Accum(empList);
					}					
				};
			}
		}

		@Override
		public Coder<Accum> getAccumulatorCoder(CoderRegistry registry, Coder<TableRow> inputCoder) {
			return Accum.getCoder();
		}

		@Override
		public Accum createAccumulator() {
			return new Accum();
		}

		@Override
		public Accum addInput(Accum accumulator, TableRow input) {
			accumulator.empList.add(input);
			return accumulator;
		}

		@Override
		public Accum mergeAccumulators(Iterable<Accum> accumulators) {
			Accum merged = createAccumulator();
			for (Accum accum : accumulators) {
				for (TableRow r : accum.empList)
				{
					merged.empList.add(r);
				}
			}
			return merged;
		}

		@Override
		public TableRow extractOutput(Accum accumulator) {

			TableRow r = null;
			List<TableRow> deptList = new ArrayList<TableRow>();

			for(TableRow tr : accumulator.empList)
			{
				if(r==null)
				{
					r = new TableRow();	
					r.set("empId", tr.get("empId"));				
					r.set("name", tr.get("name"));
				}
				
				TableRow dept = new TableRow()
						.set("deptno", tr.get("deptno"))
						.set("joindate", tr.get("joindate"));

				deptList.add(dept);
			}

			r.set("dept",deptList);
			return r;
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
