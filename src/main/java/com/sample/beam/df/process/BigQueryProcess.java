package com.sample.beam.df.process;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.sample.beam.df.utils.Utils;

public class BigQueryProcess<T extends SpecificRecordBase> extends DoFn<T, TableRow> {

	private static final long serialVersionUID = 1462827258689031685L;
	private static final Logger LOG = LoggerFactory.getLogger(BigQueryProcess.class);
	
	public static TableSchema getSchema(Schema schema) {
		TableSchema bqSchema = new TableSchema().setFields(new ArrayList<TableFieldSchema>() {
			{
				for(Field f : schema.getFields())
				{
					if(f.schema().getType().equals(Type.UNION))
					{
						List<Schema> typeList = f.schema().getTypes();
						for (Schema t : typeList)
						{
							if(t.getType().equals(Type.NULL))
								continue;

							add(new TableFieldSchema().setName(f.name()).setType(getBqType(t, f)));
						}
					} else
						add(new TableFieldSchema().setName(f.name()).setType(getBqType(f.schema(), f)));
				}	           
			}
		});
		
		return bqSchema;
	}

	static String getBqType(Schema t, Field f)
	{
		if(t.getType().equals(Type.STRING))
			return "STRING";
		else if(t.getType().equals(Type.FLOAT) || t.getType().equals(Type.DOUBLE))
			return "FLOAT";
		else if(t.getType().equals(Type.LONG) && 
				f.schema().getLogicalType() != null && f.schema().getLogicalType().getName().equals("timestamp-millis"))
			return "DATETIME";	
		else if(t.getType().equals(Type.INT) && 
				f.schema().getLogicalType() != null && f.schema().getLogicalType().getName().equals("date"))
			return "DATE";	
		else if(t.getType().equals(Type.INT) || t.getType().equals(Type.LONG))
			return "INTEGER";
		else if(t.getType().equals(Type.BOOLEAN))
			return"BOOL";
		else	
		{
			LOG.error("No BQ type found for:"+t.getType());
			return null;
		}	
	}
	
	public static TableRow createTableRow(Schema schema, SpecificRecordBase msg) {
		
		// LOG.info("msg is:"+msg);
		TableRow bqrow = new TableRow();
		for(Field f : schema.getFields())
		{
			if(msg.get(f.name()) == null)
					continue;
					
			if(bqrow.get(f.name()) instanceof org.joda.time.DateTime)
				bqrow.set(f.name(), Utils.dateMsFormatter.print((DateTime)msg.get(f.name())));
			else
				bqrow.set(f.name(), msg.get(f.name()).toString());
		}
		
		return bqrow;
	}
}
