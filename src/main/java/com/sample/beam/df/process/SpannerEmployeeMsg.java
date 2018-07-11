package com.sample.beam.df.process;

import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.Date;
import com.google.cloud.spanner.Mutation;
import com.sample.beam.df.shared.Employee;
import com.sample.beam.df.utils.Utils;

public class SpannerEmployeeMsg extends DoFn<Employee, Mutation> {

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(SpannerEmployeeMsg.class);

	@ProcessElement
	public void processElement(DoFn<Employee, Mutation>.ProcessContext c) throws Exception {

		try {
			Employee e = c.element();
			c.output(Mutation.newInsertOrUpdateBuilder("emp")
					.set("emp_id").to(e.id)
					.set("first_name").to(String.valueOf(e.getFirstName()))
					.set("birth_date").to(Date.parseDate(Utils.dateformatter.print(e.getBday())))
					.build());

		} catch(Exception e)
		{
			LOG.error("Exception in processing packet:"+e.getMessage(), e);
		}
	}
}
