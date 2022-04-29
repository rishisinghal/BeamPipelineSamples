package com.sample.beam.df.process;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.sample.beam.df.utils.BigQueryAvroUtils;
import org.apache.avro.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.kitesdk.data.spi.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class DynamicJsonProcess extends DoFn<String, String> {

    private static final long serialVersionUID = 1462827258689031685L;
    private static final Logger LOG = LoggerFactory.getLogger(DynamicJsonProcess.class);

    @ProcessElement
    public void processElement(ProcessContext context) {
        try {
//            JSONObject jsonObject = new JSONObject(context.element());
            Map oldJSONObject = new Gson().fromJson(context.element(), Map.class);

            if(oldJSONObject.containsKey("retryProcessingRow") &&
                    (double)oldJSONObject.get("retryProcessingRow") > 0) {
                context.output(context.element());
            } else {

                JsonObject newJSONObject = iterateJSON(oldJSONObject);
                newJSONObject.addProperty("retryProcessingRow", 0);
                Gson someGson = new Gson();
                String outputJson = someGson.toJson(newJSONObject);
                LOG.info("Converted JSON:" + outputJson);
                context.output(outputJson);
            }
        } catch (Exception e) {
            LOG.error("Error setting output context for ", e);
        }
    }

    private static JsonObject iterateJSON(Map JSONData) {
        JsonObject newJSONObject = new JsonObject();
        Set jsonKeys = JSONData.keySet();
        Iterator<?> keys = jsonKeys.iterator();
        while(keys.hasNext()) {
            String currentKey = (String) keys.next();
            String newKey = currentKey.replaceAll(" ", "");
            newKey = newKey.substring(0, 1).toLowerCase() + newKey.substring(1);
            if (JSONData.get(currentKey) instanceof Map) {
                JsonObject currentValue = iterateJSON((Map) JSONData.get(currentKey));
                newJSONObject.add(currentKey, currentValue);
            } else {
                Object currentValue = JSONData.get(currentKey);
                if(currentValue instanceof String)
                    newJSONObject.addProperty(newKey, (String)currentValue);
                else if(currentValue instanceof Double || currentValue instanceof Integer )
                    newJSONObject.addProperty(newKey, (Number)currentValue);
                else if(currentValue instanceof Boolean)
                    newJSONObject.addProperty(newKey, (Boolean) currentValue);
                else if(currentValue instanceof ArrayList) {
                    JsonArray dlist = new JsonArray();
                    ((ArrayList<?>) currentValue).forEach( v -> {

                        if(v instanceof String)
                            dlist.add((String) v);
                        else if(v instanceof Double || currentValue instanceof Integer )
                            dlist.add((Number) v);
                        else if(v instanceof Boolean)
                            dlist.add((Boolean) v);
                        else if(v instanceof Map) {
                            JsonObject cv = iterateJSON((Map) v);
                            dlist.add(cv);
                        }
                    });
                    newJSONObject.add(newKey,dlist);
                }
            }
        }
        return newJSONObject;
    }

    public static void main(String[] args) throws Exception {
        String input = "{\"pjsonpayload\": {\"userId\": \"username4100\", \"appInstanceId\": \"username4100\", " +
                "\"appKey\": \"8WQNAOZUQo-y5S7-o2T6Ng\", \"type\": \"event\", \"source\": \"import\", " +
                "\"event\": \"structure sent to ChemPlanner\", \"sentEpoch\": 1563339285781, \"collectedEpoch\": 1563339284567, " +
                "\"localDeviceTime\": \"2019-07-17T00:54:44.567-04:00\", " +
                "\"attributes\": {\"content_member_type\": null, \"number_of_content_members\": null, " +
                "\"number_of_content_members_delivered\": null, " +
                "\"hierarchical_node_above_credential_identity\": \"0000501927\", " +
                "\"initiating_action_id\": \"64c79072-ed6c-4de4-8e58-44dc48ea4225\"}}, " +
                "\"pContext\": {\"receivedEpoch\": 1563339285781, \"messageId\": \"8af0539b-ff1a-4dd4-88a7-dc8b0b587647\"}}";

        String input1 = "{\n" + "\"name\" : \"testarray\",\n" + "\"age\" : 11,\n" + "\"cityStayed\" : [\"blore\", \"delhi\"]\n" + "}";
        String input2 = "{\n" + "\"name\" : \"testarray\",\n" + "\"age\" : 11,\n" + "\"cityStayed\" : [21, 32 ]}";
        String input3 = "{\n" + "\"name\" : \"testarray\",\n" + "\"age\" : 11,\n" +
                "\"cityStayed\" : [{\"cname\" : \"abcd\",\"cage\" : 22}, {\"cname\" : \"eewwe\",\"cage\" : 24} ]}";


        Map oldJSONObject = new Gson().fromJson(input3, Map.class);
        JsonObject newJSONObject = iterateJSON(oldJSONObject);
        Gson someGson = new Gson();
        String outputJson = someGson.toJson(newJSONObject);
        LOG.info("Converted JSON:"+outputJson);

        Schema avroSchema = JsonUtil.inferSchema(JsonUtil.parse(outputJson), "myschema");
        LOG.info("avroSchema:"+avroSchema);

        TableSchema tableSchema = BigQueryAvroUtils.getTableSchema(avroSchema);
        LOG.info("tableSchema:"+tableSchema.toPrettyString());
    }
}
