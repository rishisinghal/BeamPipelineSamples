package com.sample.beam.df.utils;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.sample.beam.df.shared.TableMetaData;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class TableRowWithSchemaCoder extends CustomCoder<TableRowWithSchema> {

    // Coders for the member types
    private static final AvroCoder<TableMetaData> TABLE_METADATA_CODER = AvroCoder.of(TableMetaData.class);
    private static final GenericJsonCoder<TableSchema> TABLE_SCHEMA_CODER =
            GenericJsonCoder.of(TableSchema.class);
    private static final TableRowJsonCoder TABLE_ROW_CODER = TableRowJsonCoder.of();

    // Singleton instances
    private static final TableRowWithSchemaCoder INSTANCE = new TableRowWithSchemaCoder();
    private static final TypeDescriptor<TableRowWithSchema> TYPE_DESCRIPTOR =
            new TypeDescriptor<TableRowWithSchema>() {
            };

    public static TableRowWithSchemaCoder of() {
        return INSTANCE;
    }

    public static void registerCoder(Pipeline pipeline) {
        TableRowWithSchemaCoder coder = TableRowWithSchemaCoder.of();

        CoderRegistry coderRegistry = pipeline.getCoderRegistry();
        coderRegistry.registerCoderForType(coder.getEncodedTypeDescriptor(), coder);
    }

    @Override
    public void encode(TableRowWithSchema value, OutputStream outStream) throws CoderException, IOException {
        if (value == null) {
            throw new CoderException("The TableRowWithSchemaCoder cannot encode a null object!");
        }

        TABLE_METADATA_CODER.encode(value.getTableMetaData(), outStream);
        TABLE_SCHEMA_CODER.encode(value.getTableSchema(), outStream);
        TABLE_ROW_CODER.encode(value.getTableRow(), outStream);

    }

    @Override
    public TableRowWithSchema decode(InputStream inStream) throws CoderException, IOException {
        TableMetaData tableMetaData = TABLE_METADATA_CODER.decode(inStream);
        TableSchema tableSchema = TABLE_SCHEMA_CODER.decode(inStream);
        TableRow tableRow = TABLE_ROW_CODER.decode(inStream);

        return TableRowWithSchema.newBuilder()
                .withTableMetaData(tableMetaData)
                .withSchema(tableSchema)
                .withTableRow(tableRow)
                .build();
    }

    @Override
    public TypeDescriptor<TableRowWithSchema> getEncodedTypeDescriptor() {
        return TYPE_DESCRIPTOR;
    }

    @Override
    public void verifyDeterministic() {

    }
}
