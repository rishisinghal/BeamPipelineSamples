package com.sample.beam.df.utils;

import com.google.api.client.json.GenericJson;
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.Field;
import com.sample.beam.df.process.DynamicJsonProcess;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

/**
 * The {@link BigQueryAvroUtils} class provides utilities for converting records from Avro to {@link
 * TableRow} objects for insertion into BigQuery.
 */
public class BigQueryAvroUtils {
    private static final Logger LOG = LoggerFactory.getLogger(BigQueryAvroUtils.class);

    /**
     * Converts an Avro schema into a BigQuery {@link TableSchema} object.
     *
     * @param schema The Avro schema to convert.
     * @return The equivalent schema as a {@link TableSchema} object.
     */
    public static TableSchema getTableSchema(Schema schema) {
        return new TableSchema().setFields(getFieldsSchema(schema.getFields()));
    }

    /**
     * Converts an Avro {@link GenericRecord} into a BigQuery {@link TableRow} object. NOTE: At this
     * time Arrays and logical Avro types are not supported for conversion.
     *
     * @param record The Avro record to convert.
     * @return The equivalent record as a {@link TableRow} object.
     */
    public static TableRow getTableRow(GenericRecord record) {
        TableRow row = new TableRow();
        encode(record, row);
        return row;
    }

    /**
     * Converts an Avro {@link GenericRecord} into a BigQuery {@link TableCell} record object.
     *
     * @param record The record to convert.
     * @return The equivalent record as a {@link TableCell} object.
     */
    public static TableCell getTableCell(GenericRecord record) {
        TableCell cell = new TableCell();
        encode(record, cell);
        return cell;
    }

    /**
     * Encodes a {@link GenericRecord} as {@link TableRow} object. NOTE: At this * time Arrays and
     * logical Avro types are not supported for conversion.
     *
     * @param record The Avro record to encode.
     * @param row    The {@link TableRow} object which will be populated with the encoded record.
     */
    private static void encode(GenericRecord record, GenericJson row) {
        Schema schema = record.getSchema();
        schema
                .getFields()
                .forEach(
                        field -> {
                            Type type = field.schema().getType();
                            switch (type) {
                                case MAP:
                                case RECORD:
                                    row.set(field.name(), getTableCell((GenericRecord) record.get(field.pos())));
                                    break;
                                case INT:
                                    row.set(field.name(), ((Number) record.get(field.pos())).intValue());
                                    break;
                                case LONG:
                                    row.set(field.name(), ((Number) record.get(field.pos())).longValue());
                                    break;
                                case FLOAT:
                                case DOUBLE:
                                    row.set(field.name(), ((Number) record.get(field.pos())).doubleValue());
                                    break;
                                case BOOLEAN:
                                case FIXED:
                                case BYTES:
                                    row.set(field.name(), record.get(field.pos()));
                                    break;
                                default:
                                    row.set(field.name(), String.valueOf(record.get(field.pos())));
                            }
                        });
    }

    /**
     * Converts a list of Avro fields to a list of BigQuery {@link TableFieldSchema} objects.
     *
     * @param fields The Avro fields to convert to a BigQuery schema.
     * @return The equivalent fields which can be used to populate a BigQuery schema.
     */
    private static List<TableFieldSchema> getFieldsSchema(List<Schema.Field> fields) {
        return fields
                .stream()
                .map(
                        field -> {
                            TableFieldSchema column = new TableFieldSchema().setName(field.name());

                            if(field.name().equalsIgnoreCase("receivedAt") ||
                                    field.name().equalsIgnoreCase("sentAt") ||
                                    field.name().equalsIgnoreCase("timestamp") ||
                                    field.name().equalsIgnoreCase("originalTimestamp")) {
                                column.setType("TIMESTAMP");
                                return column;
                            }

                            if(field.name().equalsIgnoreCase("name")) {
                                column.setMode("REQUIRED");
                            }

                            Schema colSchema = field.schema();
                            Type type = colSchema.getType();
                            switch (type) {
                                case MAP:
                                case RECORD:
                                    column.setType("STRUCT");
                                    column.setFields(getFieldsSchema(field.schema().getFields()));
                                    break;
                                case INT:
                                case LONG:
                                    column.setType("INT64");
                                    break;
                                case BOOLEAN:
                                    column.setType("BOOL");
                                    break;
                                case FLOAT:
                                    column.setType("FLOAT64");
                                    break;
                                case DOUBLE:
                                    column.setType("NUMERIC");
                                    break;
                                case FIXED:
                                case BYTES:
                                    column.setType("BYTES");
                                    break;
                                case ARRAY:
                                    if(colSchema.getElementType().getType()==Type.INT ||
                                            colSchema.getElementType().getType()==Type.LONG) {
                                        column.setType("INT64");
                                    } else if(colSchema.getElementType().getType()==Type.BOOLEAN) {
                                        column.setType("BOOL");
                                    } else if(colSchema.getElementType().getType()==Type.FLOAT) {
                                        column.setType("FLOAT64");
                                    } else if(colSchema.getElementType().getType()==Type.DOUBLE) {
                                        column.setType("NUMERIC");
                                    } else if(colSchema.getElementType().getType()==Type.FIXED ||
                                            colSchema.getElementType().getType()==Type.BYTES) {
                                        column.setType("BYTES");
                                    } else if(colSchema.getElementType().getType()==Type.RECORD ||
                                            colSchema.getElementType().getType()==Type.MAP) {
                                        column.setType("STRUCT");
//                                        LOG.info("Schema:" +field.schema().getElementType().getFields());
                                        column.setFields(getFieldsSchema(field.schema().getElementType().getFields()));
                                    } else {
                                        column.setType("STRING");
                                    }
                                    column.setMode("REPEATED");
                                    break;
                                default:
                                    column.setType("STRING");
                            }
                            return column;
                        })
                .collect(Collectors.toList());
    }
}
