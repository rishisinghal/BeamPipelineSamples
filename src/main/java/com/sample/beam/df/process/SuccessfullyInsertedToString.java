package com.sample.beam.df.process;

import com.google.api.services.bigquery.model.TableRow;
import com.google.gson.Gson;
import com.sample.beam.df.utils.TableRowWithSchema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;

public class SuccessfullyInsertedToString extends DoFn<KV<BigInteger, Iterable<TableRowWithSchema>>,
        String> {

    private static final Logger LOG = LoggerFactory.getLogger(SuccessfullyInsertedToString.class);

    @ProcessElement
    public void processElement(ProcessContext context) {

        try {
            KV<BigInteger, Iterable<TableRowWithSchema>> inputColl = context.element();
            Iterable<TableRowWithSchema> mutatedRows = inputColl.getValue();
            int counter = 0;
            TableRow trow = null;
            for (TableRowWithSchema tr : mutatedRows) {
                trow = tr.getTableRow();
                counter++;
            }

            if(counter==1) {
                String jsonRow =  new Gson().toJson(trow.getUnknownKeys());
                context.output(jsonRow);
            }

        } catch (Exception e) {
            LOG.error("Exception in tablerow conversion", e);
        }
    }
}