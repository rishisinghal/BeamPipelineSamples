package com.sample.beam.df.process;

import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.sample.beam.df.shared.DeviceTelemetry;

public class BigQueryTelemetryProcess extends BigQueryProcess<DeviceTelemetry> {

	private static final long serialVersionUID = 1462827258689031685L;
	private static final Logger LOG = LoggerFactory.getLogger(BigQueryTelemetryProcess.class);

	@ProcessElement
	public void processElement(DoFn<DeviceTelemetry, TableRow>.ProcessContext c) throws Exception {

		DeviceTelemetry msg = (DeviceTelemetry) c.element();

		//return a new TableRow
		TableRow btrow = BigQueryProcess.createTableRow(DeviceTelemetry.getClassSchema(), msg);
		c.output(btrow);
	}
}
