package com.sample.beam.df.process;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.opencsv.CSVParser;
import com.sample.beam.df.shared.Employee;
import com.sample.beam.df.utils.Utils;

public class CsvEmpTableRowProcess extends DoFn<String, KV<Integer, TableRow>> {

	private static final long serialVersionUID = 1462827258689031685L;
	private static final Logger LOG = LoggerFactory.getLogger(CsvEmpTableRowProcess.class);
	private CSVParser csvParser;
	
	@Setup
    public void initOpenCsv() {
		csvParser = new CSVParser();
    }
	
	@ProcessElement
	public void processElement(DoFn<String, KV<Integer, TableRow>>.ProcessContext c) throws Exception {

		try {
			String[] parts = csvParser.parseLine(c.element());
			int key = Integer.parseInt(parts[0]);

			TableRow r = new TableRow();
			r.set("empId", Integer.parseInt(parts[0]));
			r.set("name", parts[1]);
			r.set("deptno", parts[3]);
			r.set("joindate", parts[4]);
			
			c.output(KV.of(key, r));
		} catch(Exception e)
		{
			LOG.error("Exception in processing packet:"+e.getMessage(), e);
		}
	}
}
