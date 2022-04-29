package com.sample.beam.df.process;

import com.google.gson.Gson;
import com.sample.beam.df.utils.TableRowWithSchema;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrintProcess extends DoFn<TableRowWithSchema, String> {
	
	private static final long serialVersionUID = 1462827258689031685L;
	private static final Logger LOG = LoggerFactory.getLogger(PrintProcess.class);

	@ProcessElement
	public void processElement(ProcessContext c) {
		LOG.info("Element is:"+c.element().getTableRow().toString());
		String jsonRow =  new Gson().toJson(c.element().getTableRow().getUnknownKeys());
		c.output(jsonRow);
	}
}