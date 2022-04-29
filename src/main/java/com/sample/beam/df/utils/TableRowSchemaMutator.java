package com.sample.beam.df.utils;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.Field.Mode;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.gson.Gson;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.kitesdk.data.spi.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link TableRowWithSchema} mutator.
 */
public class TableRowSchemaMutator extends DoFn<KV<String, Iterable<TableRowWithSchema>>, TableRowWithSchema> {

    private transient BigQuery bigQuery;
    private static final Logger LOG = LoggerFactory.getLogger(TableRowSchemaMutator.class);

    public TableRowSchemaMutator(BigQuery bigQuery) {
        this.bigQuery = bigQuery;
    }

    @Setup
    public void setup() throws IOException {
        if (bigQuery == null) {
            bigQuery = BigQueryOptions.newBuilder().setCredentials(GoogleCredentials.getApplicationDefault())
                    .build()
                    .getService();
        }
    }

    @ProcessElement
    public void processElement(ProcessContext context) {

        try {
            KV<String, Iterable<TableRowWithSchema>> inputColl = context.element();
            Iterable<TableRowWithSchema> mutatedRows = inputColl.getValue();

            TableRowWithSchema trs = mutatedRows.iterator().next();
            LOG.info(" Table from metadata:" + trs.getTableMetaData().getTable());
            LOG.info("Check Row:" + trs.getTableRow().toString());
            LOG.info("Check Schema:" + trs.getTableSchema().toString());

            // Retrieve the table schema
            TableId tableId = TableId.of(trs.getTableMetaData().getProject(),
                    trs.getTableMetaData().getDataset(), trs.getTableMetaData().getTable());

            Table table = bigQuery.getTable(tableId);
            LOG.info("Check table:" + tableId.toString());

            checkNotNull(table, "Failed to find table to mutate: " + tableId.toString());

            TableDefinition tableDef = table.getDefinition();
            Schema schema = tableDef.getSchema();
            LOG.info("Existing table schema is:" + schema.toString());

            checkNotNull(table, "Unable to retrieve schema for table: " + tableId.toString());

            processAllRows(schema, mutatedRows, table, tableDef);
            // Compare the records to the known table schema
//            Set<Field> additionalFields = getAdditionalFields(schema, mutatedRows);
//            LOG.info("additionalFields:" + additionalFields.toString());
//
//            // Update the table schema for the new fields
//            schema = addFieldsToSchema(schema, additionalFields);
//            LOG.info("Updated Schema:" + schema.toString());
//
//            table.toBuilder()
//                    .setDefinition(tableDef.toBuilder().setSchema(schema).build())
//                    .build()
//                    .update();

            // Pass all rows downstream now that the schema of the output table has been mutated.
//            mutatedRows.forEach(context::output);

            inputColl.getValue().forEach(context::output);
        } catch (Exception e) {
            LOG.error("Exception in schema processing", e);
        }
    }

    private void processAllRows(Schema schema, Iterable<TableRowWithSchema> mutatedRows, Table table,
                                      TableDefinition tableDef) {

        ObjectMapper objectMapper =  new ObjectMapper();
        String JSON_SCHEMA = null;

//        for(TableRowWithSchema row : mutatedRows) {
        TableRowWithSchema row = mutatedRows.iterator().next();
            LOG.info("mutatedRow is:" + mutatedRows.iterator().next().getTableSchema().getFields().toString());

            try {
                String json =  new Gson().toJson(row.getTableSchema().getFields());
                LOG.info("JSON is:" + json);

                if(JSON_SCHEMA == null) {
                    JSON_SCHEMA = json;
                } else {
                    if(json.equalsIgnoreCase(JSON_SCHEMA)) {
                        return;
                    }
                }

                JsonNode jsonNode = objectMapper.readTree(json);
                List<TableFieldSchema> tfList = objectMapper
                        .convertValue(jsonNode, new TypeReference<List<TableFieldSchema>>() {});

                AtomicBoolean updated = new AtomicBoolean(false);
                Set<Field> mergedSchemaFields = mergeSchema(schema.getFields(), tfList, updated);
                if (!updated.get()) {
                    //BQ Schema is already upto date
                    return;
                }

                //Update mergedSchema with top level table fields that are not in incoming payload
                for (Field field : schema.getFields()) {
                    if (!isExistingField(FieldList.of(mergedSchemaFields), field.getName())) {
                        mergedSchemaFields.add(field);
                    }
                }

                // Update the table schema with the new fields
                schema = Schema.of(mergedSchemaFields);
                LOG.info("Updated schema to update :" + schema.toString());
                table.toBuilder().setDefinition(tableDef.toBuilder()
                            .setSchema(schema).build())
                            .build().update();


            } catch (IOException e) {
                LOG.error("Exception in new fields processing", e);
            }
//        }
    }

    private static Set<Field> mergeSchema(FieldList fieldList, List<TableFieldSchema> payloadSchema, AtomicBoolean updated) throws IOException {
        ObjectMapper objectMapper =  new ObjectMapper();
        Set<Field> mergedSchemaFields = Sets.newHashSet();
        for (TableFieldSchema payloadFields : payloadSchema) {
            if (!isExistingField(fieldList, payloadFields.getName())) {
                Mode fieldMode = Field.Mode.NULLABLE;
                if(payloadFields.getMode() != null)
                    fieldMode = Mode.valueOf(payloadFields.getMode());

                if (payloadFields.getType().equalsIgnoreCase("STRUCT")) {

                    String json =  new Gson().toJson(payloadFields.getFields());
                    LOG.info("payloadFields struct is:" + json);
                    LOG.info("payloadFields struct mode is:" + payloadFields.getMode());

                    JsonNode jsonNode = objectMapper.readTree(json);
                    List<TableFieldSchema> tfList = objectMapper
                            .convertValue(jsonNode, new TypeReference<List<TableFieldSchema>>() {});

                    Set<Field> subFields = getSubFields(tfList);
                    if (!subFields.isEmpty()) {
                        updated.set(true);
                        mergedSchemaFields.add(Field.newBuilder(payloadFields.getName(),
                                StandardSQLTypeName.valueOf(payloadFields.getType()),
                                FieldList.of(subFields))
                                .setMode(fieldMode)
                                .build());
                    }
                } else {
                    LOG.info("NON struct field is:" + payloadFields.getName()+ " with mode:"+fieldMode.name());
                    updated.set(true);
                    mergedSchemaFields.add(Field.newBuilder(payloadFields.getName(),
                            StandardSQLTypeName.valueOf(payloadFields.getType()))
                            .setMode(fieldMode)
                            .build());
                }
            } else if (payloadFields.getType().equalsIgnoreCase("STRUCT")) {

                Mode fieldMode = Field.Mode.NULLABLE;
                if(payloadFields.getMode() != null)
                    fieldMode = Mode.valueOf(payloadFields.getMode());

                String json =  new Gson().toJson(payloadFields.getFields());
                LOG.info("JSON struct is:" + json);
                JsonNode jsonNode = objectMapper.readTree(json);
                List<TableFieldSchema> tfStructList = objectMapper
                        .convertValue(jsonNode, new TypeReference<List<TableFieldSchema>>() {});

                Set<Field> recordSubFields = mergeSchema(fieldList.get(payloadFields.getName()).getSubFields(),
                        tfStructList, updated);
                for (Field existingField : fieldList.get(payloadFields.getName()).getSubFields()) {
                    if (!isExistingField(FieldList.of(recordSubFields), existingField.getName())) {
                        recordSubFields.add(existingField);
                    }
                }
                mergedSchemaFields.add(Field.newBuilder(payloadFields.getName(),
                        StandardSQLTypeName.valueOf(payloadFields.getType()),
                        FieldList.of(recordSubFields))
                        .setMode(fieldMode)
                        .build());

            } else {
                //Field already exists, compare the field schema
                /*if(!fieldList.get(payloadFields.getName()).getType().getStandardType().toString().equals(payloadFields.getType())){
                    throw new FieldMismatchException(payloadFields.getName()+" Field type different. Payload: "+payloadFields.getType()+
                            ". Table field "+fieldList.get(payloadFields.getName()).getType().getStandardType().toString());
                }*/
                LOG.info("Existing field:"+payloadFields.getName() + " with details:"
                        +fieldList.get(payloadFields.getName()).toString());

                mergedSchemaFields.add(fieldList.get(payloadFields.getName()));
            }
        }
        return mergedSchemaFields;
    }

    private static Set<Field> getSubFields(List<TableFieldSchema> tableSchema) throws IOException {
        Set<Field> additionalFields = Sets.newHashSet();
        ObjectMapper objectMapper =  new ObjectMapper();

        for (TableFieldSchema field : tableSchema) {
            Mode fieldMode = Field.Mode.NULLABLE;
            if(field.getMode() != null)
                fieldMode = Mode.valueOf(field.getMode());

            if (field.getType().equalsIgnoreCase("STRUCT")) {

                String json =  new Gson().toJson(field.getFields());
                JsonNode jsonNode = objectMapper.readTree(json);
                List<TableFieldSchema> tfList = objectMapper
                        .convertValue(jsonNode, new TypeReference<List<TableFieldSchema>>() {});

                additionalFields.add(Field.newBuilder(field.getName(), StandardSQLTypeName.valueOf(field.getType()),
                        FieldList.of(getSubFields(tfList)))
                        .setMode(fieldMode)
                        .build());
            } else {
                additionalFields.add(Field.newBuilder(field.getName(), StandardSQLTypeName.valueOf(field.getType()))
                        .setMode(fieldMode)
                        .build());
            }

        }
        return additionalFields;
    }

    /**
     * Retrieves the fields which have been added to the schema across all of the mutated rows.
     *
     * @param schema      The schema to validate against.
     * @param mutatedRows The records which have mutated.
     * @return A unique set of fields which have been added to the schema.
     */
    private Set<Field> getAdditionalFields(Schema schema, Iterable<TableRowWithSchema> mutatedRows) {

        // Compare the existingSchema to the mutated rows
        FieldList fieldList = schema.getFields();
        Set<Field> additionalFields = Sets.newHashSet();
        ObjectMapper objectMapper =  new ObjectMapper();

        mutatedRows.forEach(
                row -> {
                    LOG.info("mutatedRow is:" + row.getTableSchema().getFields().toString());

                    try {
                        String json =  new Gson().toJson(row.getTableSchema().getFields());
//                        String json = objectMapper.writeValueAsString(row.getTableSchema().getFields());
                        LOG.info("JSON is:" + json);

                        JsonNode jsonNode = objectMapper.readTree(json);
                        List<TableFieldSchema> tfList = objectMapper
                                .convertValue(jsonNode, new TypeReference<List<TableFieldSchema>>() {});

                        for (TableFieldSchema payloadFields : tfList) {
                            LOG.info("Check field :" + payloadFields.getName());

                            // If the field is a record it has a special processing
                            //
                            if (LegacySQLTypeName.valueOf(payloadFields.getType()) == LegacySQLTypeName.RECORD) {


                                // If this is a new RECORD that needs to be added
                                if (!isExistingField(fieldList, payloadFields.getName())) {
                                    Set<Field> additionalRecFields = Sets.newHashSet();

                                    List<TableFieldSchema> tfRecordList = payloadFields.getFields();
                                    for(TableFieldSchema tfs : tfRecordList) {
                                        additionalRecFields.add(Field.of(payloadFields.getName(),
                                                LegacySQLTypeName.valueOf(payloadFields.getType()))
                                                .toBuilder()
                                                .setMode(Mode.NULLABLE)
                                                .build());
                                    }
                                    Field recField = Field.of(payloadFields.getName(),
                                            LegacySQLTypeName.RECORD)
                                            .toBuilder()
                                            .setMode(Mode.NULLABLE)
                                            .build();

                                }
                                // else if this record exists check if there are any new fields added
                                //

                            } else if (!isExistingField(fieldList, payloadFields.getName())) {
                                LOG.info("add field is:" + payloadFields.getName());
                                additionalFields.add(Field.of(payloadFields.getName(),
                                        LegacySQLTypeName.valueOf(payloadFields.getType()))
                                        .toBuilder()
                                        .setMode(Mode.NULLABLE)
                                        .build());
                            }


                        }
                    } catch (IOException e) {
                        LOG.error("Exception in new fields processing", e);
                    }
                });

        return additionalFields;
    }

    /**
     * Checks whether the field name exists within the field list.
     *
     * @param fieldList The field list to validate the field against.
     * @param fieldName The field to check for.
     * @return True if the fieldName exists within the field list, false otherwise.
     */
    private static boolean isExistingField(FieldList fieldList, String fieldName) {
        try {
            fieldList.get(fieldName);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    /**
     * Adds additional fields to an existing schema and returns a new schema containing all existing
     * and additional fields.
     *
     * @param schema           The pre-existing schema to add fields to.
     * @param additionalFields The new fields to be added to the schema.
     * @return A new schema containing the existing and new fields added to the schema.
     */
    private Schema addFieldsToSchema(Schema schema, Set<Field> additionalFields) {
        List<Field> newFieldList = Lists.newArrayList();

        // Add the existing fields to the schema fields.
        newFieldList.addAll(schema.getFields());

        // Add all new fields to the schema fields.
        newFieldList.addAll(additionalFields);

        return Schema.of(newFieldList);
    }
}