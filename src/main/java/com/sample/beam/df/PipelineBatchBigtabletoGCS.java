/**
 * ===========================================
 * The code is for DEMO purpose only and it is
 * not intended to be put in production
 * ===========================================
 */

package com.sample.beam.df;

import com.google.bigtable.v2.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.sample.beam.df.utils.DatabaseOptions;
import com.sample.beam.df.utils.Utils;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.*;
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

import java.util.ArrayList;
import java.util.List;

public class PipelineBatchBigtabletoGCS {
    private static final Logger LOG = LoggerFactory.getLogger(PipelinePubsubSpanner.class);
    private static final String DEFAULT_CONFIG_FILE = "application1.properties";
    private static Configuration config;
    private DatabaseOptions options;
    private static final String BT_COLUMN_FAMILY_NAME = "stats_summary";
    private static final int MAX_ROW_SIZE = 64;


    public static void main(String[] args) {

        PipelineBatchBigtabletoGCS sp = new PipelineBatchBigtabletoGCS();
        String propFile = null;

        if (args.length > 0) // For custom properties file
            propFile = args[0];
        else
            propFile = DEFAULT_CONFIG_FILE;

        sp.init(propFile);
        sp.run();
    }

    public void run() {
        //create Pipeline with options
        Pipeline pipeline = Pipeline.create(options);
        doDataProcessing(pipeline);
        pipeline.run();
    }

    public void doDataProcessing(Pipeline pipeline) {
        //read rows from BigTable table
        PCollection<Row> btRows = pipeline.apply(BigtableIO.read()
                .withProjectId(config.getString("gcp.projectId"))
                .withInstanceId(config.getString("bt.instanceId"))
                .withTableId(config.getString("bt.tableId"))
        );

        Family dedede = Family.newBuilder().addColumns(Column.newBuilder()
                .addCells(Cell.newBuilder().setValue(ByteString.copyFromUtf8("value")).build()).build()).build();
        Row expectedRow = Row.newBuilder().setKey(ByteString.copyFromUtf8("key1"))
                .setFamilies(0,dedede).build();
        List<Row> rowList = new ArrayList<>();
        rowList.add(expectedRow);
        PCollection<Row> pt = pipeline.apply(Create.of(rowList));

        if (config.getBoolean("bt.dryRun")) {
            PCollection<String> toDeleteRows = btRows.apply(ParDo.of(new BigtableToStringFn()));
            // Don't delete just write to GCS bucket
            toDeleteRows.apply("Write to GCS", TextIO.write().to(config.getString("gcs.writeFolderPath"))
                    .withWindowedWrites()
                    .withNumShards(1)
                    .withSuffix(".csv")
                    .withCompression(Compression.UNCOMPRESSED));
        } else {
            // delete the BT rows
            PCollection<KV<ByteString, Iterable<Mutation>>> toDeleteRows = btRows.apply(ParDo.of(new FindBigSizedRowsDoFn()));
            toDeleteRows.apply(BigtableIO.write()
                    .withProjectId(config.getString("gcp.projectId"))
                    .withInstanceId(config.getString("bt.instanceId"))
                    .withTableId(config.getString("bt.tableId"))
            );
        }
    }

    static class BigtableToStringFn extends DoFn<Row, String> {
        @ProcessElement
        public void processElement(ProcessContext context) {
            Row row = context.element();
            String result = row.getKey().toStringUtf8();
            double size = getSizeInMb(row.toByteArray().length);

            // If size is greater than 64MB add the row to collection
            if (size > MAX_ROW_SIZE) {
                context.output(result + "," + size);
            }
        }
    }

    static class FindBigSizedRowsDoFn extends DoFn<Row, KV<ByteString, Iterable<Mutation>>> {
        @ProcessElement
        public void processElement(ProcessContext context) {

            Row row = context.element();
            double size = getSizeInMb(row.toByteArray().length);

            // If size is greater than 64MB add the row to delete collection
            if (size < MAX_ROW_SIZE) {
                return;
            }

            ByteString key = row.getKey();
            List<Family> colFamilyList = row.getFamiliesList();
            for(Family fam : colFamilyList) {
                List<Column> columns = fam.getColumnsList();
                List<Mutation> mutations = new ArrayList<>(columns.size());

                for (Column column : columns) {
                    mutations.add(Mutation.newBuilder().setDeleteFromColumn(
                            Mutation.DeleteFromColumn.newBuilder()
                                    .setFamilyName(fam.getName())
                                    .setColumnQualifier(column.getQualifier()))
                            .build());

                    if (!mutations.isEmpty()) {
                        context.output(KV.of(key, mutations));
                    }
                }
            }
        }
    }

    private static double getSizeInMb(int length) {
        int mb = 1024 * 1024;
        return length / mb;
    }

    public void init(String propFile) {
        Parameters params = new Parameters();
        FileBasedConfigurationBuilder<FileBasedConfiguration> builder =
                new FileBasedConfigurationBuilder<FileBasedConfiguration>(PropertiesConfiguration.class)
                        .configure(params.properties()
                                .setFileName(propFile));
        try {
            LOG.info("Init Config start");
            config = builder.getConfiguration();

            //define pipeline options
            options = PipelineOptionsFactory.create().as(DatabaseOptions.class);

            // Set DataFlow options
            options.setAppName(config.getString("df.appName"));
            options.setStagingLocation(config.getString("gcs.urlBase") + config.getString("gcs.bucketName") +
                    "/" + config.getString("gcs.stagingLocation"));
            options.setTempLocation(config.getString("gcs.urlBase") + config.getString("gcs.bucketName") +
                    "/" + config.getString("gcs.tempLocation"));
            options.setRunner(DataflowRunner.class);
//			options.setRunner(DirectRunner.class);
            options.setRegion(config.getString("df.region"));
            options.setStreaming(false);
            options.setProject(config.getString("gcp.projectId"));
            options.setAutoscalingAlgorithm(AutoscalingAlgorithmType.THROUGHPUT_BASED);
            options.setMaxNumWorkers(config.getInt("df.maxWorkers"));
            options.setJobName(config.getString("df.baseJobName") + Utils.dateSecFormatter.format(new java.util.Date()));
        } catch (ConfigurationException cex) {
            LOG.error("Exception during initialization of properties:", cex);
        }
    }

}
