package com.sample.beam.df.process;

import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.Date;
import com.google.cloud.spanner.Mutation;
import com.sample.beam.df.shared.Employee;
import com.sample.beam.df.utils.Utils;

public class SpannerDeptMsg extends DoFn<Employee, Mutation> {

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(SpannerDeptMsg.class);

	@ProcessElement
	public void processElement(DoFn<Employee, Mutation>.ProcessContext c) throws Exception {

		try {
			Employee e = c.element();
			c.output(Mutation.newInsertOrUpdateBuilder("dept")
					.set("emp_id").to(e.id)
					.set("dept").to(String.valueOf(e.getDept()))
					.set("join_date").to(Date.parseDate(Utils.dateformatter.print(e.getHireDate())))
					.build());

		} catch(Exception e)
		{
			LOG.error("Exception in processing packet:"+e.getMessage(), e);
		}
	}
}
