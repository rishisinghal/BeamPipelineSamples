package com.sample.beam.df.process;

import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opencsv.CSVParser;
import com.sample.beam.df.shared.Employee;
import com.sample.beam.df.utils.Utils;

public class CsvEmployeeProcess extends DoFn<String, Employee> {

	private static final long serialVersionUID = 1462827258689031685L;
	private static final Logger LOG = LoggerFactory.getLogger(CsvEmployeeProcess.class);
	private CSVParser csvParser;
	
	@Setup
    public void initOpenCsv() {
		csvParser = new CSVParser();
    }
	
	@ProcessElement
	public void processElement(DoFn<String, Employee>.ProcessContext c) throws Exception {

		try {
			String[] parts = csvParser.parseLine(c.element());
			Employee emp = new Employee();
			emp.setId(Integer.parseInt(parts[0]));
			emp.setBday(Utils.dateformatter.parseLocalDate(parts[1]));
			emp.setFirstName(parts[2]);
			emp.setLastName(parts[3]);
			emp.setGender(parts[4]);
			emp.setHireDate(Utils.dateformatter.parseLocalDate(parts[5]));
			
			LOG.info("Employee row is:"+emp.toString());
			
			c.output(emp);
		} catch(Exception e)
		{
			LOG.error("Exception in processing packet:"+e.getMessage(), e);
		}
	}
}
