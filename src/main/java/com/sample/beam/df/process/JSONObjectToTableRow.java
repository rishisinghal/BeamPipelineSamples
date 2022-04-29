package com.sample.beam.df.process;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.sample.beam.df.shared.TableMetaData;
import com.sample.beam.df.utils.BigQueryAvroUtils;
import com.sample.beam.df.utils.TableRowWithSchema;
import org.apache.avro.Schema;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.kitesdk.data.spi.JsonUtil;


public class JSONObjectToTableRow extends DoFn<String, TableRowWithSchema> {
    private static final Logger LOG = LoggerFactory.getLogger(JSONObjectToTableRow.class);

    @ProcessElement
    public void processElement(ProcessContext context) {
        try {
//            JSONObject eventsBQTemplate = new JSONObject(context.element());
            String jsonRow = context.element();
            Schema avroSchema = JsonUtil.inferSchema(JsonUtil.parse(jsonRow), "myschema");

            TableSchema tableSchema = BigQueryAvroUtils.getTableSchema(avroSchema);
            TableRow tableRow = convertJsonToTableRow(jsonRow);

            TableMetaData tableMetaData = new TableMetaData();
            tableMetaData.setProject("training-sandbox-sgp");
            tableMetaData.setDataset("training");
            tableMetaData.setPartitioningKey("timestamp");
            tableMetaData.setType("event");
            tableMetaData.setTable("dynamic_events");

            TableRowWithSchema tableRowWithSchema = TableRowWithSchema.newBuilder()
                    .withSchema(tableSchema)
                    .withTableMetaData(tableMetaData)
                    .withTableRow(tableRow)
                    .build();

            context.output(tableRowWithSchema);
        } catch (Exception ex) {
            LOG.error("Exception while generating table row: " + ex.getMessage() + "; payload=" + context.element(), ex);
        }
    }

    private static TableRow convertJsonToTableRow(String json) throws IOException {
        // Parse the JSON into a {@link TableRow} object.
        try (InputStream inputStream =
                     new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))) {
            return TableRowJsonCoder.of().decode(inputStream, Coder.Context.OUTER);
        }
    }
}
