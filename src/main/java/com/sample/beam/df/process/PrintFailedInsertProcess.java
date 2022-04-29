package com.sample.beam.df.process;

import com.sample.beam.df.utils.TableRowWithSchema;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class PrintFailedInsertProcess extends DoFn<BigQueryInsertError, String> {

    private static final long serialVersionUID = 1462827258689031685L;
    private static final Logger LOG = LoggerFactory.getLogger(PrintProcess.class);

    @ProcessElement
    public void processElement(ProcessContext c) throws IOException {

        LOG.info("Reinsert failed Error is:"+c.element().getError().toPrettyString() +
                " row is:"+c.element().getRow().toString());

        c.output("Reinsert failed for:"+c.element().getError().toPrettyString() +
                " row is:"+c.element().getRow().toString());
    }
}