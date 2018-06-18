package com.sample.beam.df.process;

import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.sample.beam.df.shared.DeviceTelemetry;
import com.sample.beam.df.shared.Employee;

public class BigQueryEmployeeProcess extends BigQueryProcess<Employee> {

	private static final long serialVersionUID = 1462827258689031685L;
	private static final Logger LOG = LoggerFactory.getLogger(BigQueryEmployeeProcess.class);

	@ProcessElement
	public void processElement(DoFn<Employee, TableRow>.ProcessContext c) throws Exception {

		Employee msg = (Employee) c.element();
		
		//return a new TableRow
		TableRow btrow = BigQueryProcess.createTableRow(Employee.getClassSchema(), msg);
		c.output(btrow);
	}
}
