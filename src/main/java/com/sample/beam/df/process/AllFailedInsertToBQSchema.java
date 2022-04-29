package com.sample.beam.df.process;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.gson.Gson;
import com.sample.beam.df.shared.TableMetaData;
import com.sample.beam.df.utils.BigQueryAvroUtils;
import com.sample.beam.df.utils.TableRowWithSchema;
import org.apache.avro.Schema;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.kitesdk.data.spi.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Base64;

/**
 * From the failed row create an object of TableRowWithSchema. Check if the error is due to change in schema,
 * if it is add it the failedSchemaTupleTag tag.
 */
public class AllFailedInsertToBQSchema extends DoFn<BigQueryInsertError, KV<BigInteger, TableRowWithSchema>> {

    private static final Logger LOG = LoggerFactory.getLogger(AllFailedInsertToBQSchema.class);
    private String schemeName;
    private String portfolioKey;
    private String projName;
    private String partKey;
    private String tableType;
    private String tableName;

    public AllFailedInsertToBQSchema(String schemeName, String portfolioKey, String projName, String partKey,
                                     String tableType, String tableName) {
        this.schemeName = schemeName;
        this.portfolioKey = portfolioKey;
        this.projName = projName;
        this.partKey = partKey;
        this.tableType = tableType;
        this.tableName = tableName;
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws IOException {
        BigQueryInsertError failedInsertError = context.element();
        LOG.info("Error while inserting:"+failedInsertError.getError().toPrettyString());

        TableRow failedInsert = failedInsertError.getRow();

        LOG.info("failed row data:"+ failedInsert.toPrettyString());
        String jsonRow =  new Gson().toJson(failedInsert.getUnknownKeys());
        Schema avroSchema = JsonUtil.inferSchema(JsonUtil.parse(jsonRow), schemeName);

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

        KV<BigInteger,TableRowWithSchema> keyValue = KV.of(BigInteger.valueOf(failedInsert.hashCode()),
                    tableRowWithSchema);
        context.output(keyValue);
    }
}