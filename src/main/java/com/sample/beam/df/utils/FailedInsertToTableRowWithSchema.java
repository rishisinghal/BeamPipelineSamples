package com.sample.beam.df.utils;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableRow;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;

import com.google.api.services.bigquery.model.TableSchema;
import com.google.gson.Gson;
import com.sample.beam.df.PipelinePubsubSpanner;
import com.sample.beam.df.shared.TableMetaData;
import org.apache.avro.Schema;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.kitesdk.data.spi.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class FailedInsertToTableRowWithSchema extends DoFn<BigQueryInsertError, KV<String,TableRowWithSchema>> {

//    private PCollectionView<Map<BigInteger, TableRowWithSchema>> incomingRecordsView;
    private static final Logger LOG = LoggerFactory.getLogger(FailedInsertToTableRowWithSchema.class);
    public static final TupleTag<KV<String,TableRowWithSchema>> failedSchemaTupleTag = new TupleTag<KV<String,TableRowWithSchema>>(){};
    public static final TupleTag<KV<String,TableRowWithSchema>> otherFailedTupleTag = new TupleTag<KV<String,TableRowWithSchema>>(){};

    public FailedInsertToTableRowWithSchema() {
//        this.incomingRecordsView = incomingRecordsView;
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws IOException {
        BigQueryInsertError failedInsertError = context.element();
        LOG.info("Error during insert is:"+failedInsertError.getError().toPrettyString());

        TableRow failedInsert = failedInsertError.getRow();

//        Map<BigInteger, TableRowWithSchema> schemaMap = context.sideInput(incomingRecordsView);
//        for (Map.Entry<BigInteger, TableRowWithSchema> entry : schemaMap.entrySet())
//            LOG.info("Map key is:"+entry.getKey().toString() +
//                    " Value is:"+ entry.getValue().getTableRow().toPrettyString());

        LOG.info("failed insert:"+ failedInsert.toPrettyString());
        String jsonRow =  new Gson().toJson(failedInsert.getUnknownKeys());
        Schema avroSchema = JsonUtil.inferSchema(JsonUtil.parse(jsonRow), "myschema");

        TableSchema tableSchema = BigQueryAvroUtils.getTableSchema(avroSchema);

        TableMetaData tableMetaData = new TableMetaData();
        tableMetaData.setProject("training-sandbox-sgp");
        tableMetaData.setDataset("training");
        tableMetaData.setPartitioningKey("timestamp");
        tableMetaData.setType("event");
        tableMetaData.setTable("dynamic_events");

        TableRowWithSchema tableRowWithSchema = TableRowWithSchema.newBuilder()
                .withSchema(tableSchema)
                .withTableMetaData(tableMetaData)
                .withTableRow(failedInsert)
                .build();

        LOG.info("Schema is: "+tableRowWithSchema.getTableSchema().toString());
        if(failedInsertError.getError().getErrors().get(0).getMessage().contains("no such field")) {
            failedInsert.set("retryProcessingRow", (int)failedInsert.get("retryProcessingRow")+1);

            KV<String,TableRowWithSchema> keyValue = KV.of(
                    failedInsertError.getError().getErrors().get(0).getLocation(),
                    tableRowWithSchema);

            context.output(failedSchemaTupleTag, keyValue);
        } else {
            KV<String, TableRowWithSchema> keyValue = KV.of("", tableRowWithSchema);
            context.output(otherFailedTupleTag, keyValue);
        }
    }
}