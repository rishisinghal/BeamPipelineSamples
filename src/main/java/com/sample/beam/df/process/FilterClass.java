package com.sample.beam.df.process;

import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilterClass extends DoFn<String, String> {
	
	private static final long serialVersionUID = 1462827258689031685L;
	private static final Logger LOG = LoggerFactory.getLogger(FilterClass.class);

	@ProcessElement
	public void processElement(DoFn<String, String>.ProcessContext c) throws Exception {
		LOG.info("Element is:"+c.element());

		if(c.element()!=null)
			c.output(c.element());

	}
}