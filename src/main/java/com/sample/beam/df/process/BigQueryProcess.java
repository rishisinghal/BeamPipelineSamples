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
import com.sample.beam.df.shared.EmpDept;
import com.sample.beam.df.utils.Utils;

public class BigQueryProcess<T extends SpecificRecordBase> extends DoFn<T, TableRow> {

	private static final long serialVersionUID = 1462827258689031685L;
	private static final Logger LOG = LoggerFactory.getLogger(BigQueryProcess.class);

	public static TableSchema getTableSchema(Schema schema) {
		TableSchema ts = new TableSchema();
		List<TableFieldSchema> fields = getTableFieldSchema(schema);
		ts.setFields(fields);
		return ts;
	}

	public static List<TableFieldSchema> getTableFieldSchema(Schema schema) {
		List<TableFieldSchema> tableFieldSchemas = new ArrayList<>();
		if (schema != null) {
			for (Schema.Field field : schema.getFields()) {

				if(field.schema().getType().equals(Type.UNION))
				{
					List<Schema> typeList = field.schema().getTypes();
					for (Schema t : typeList)
					{
						if(t.getType().equals(Type.NULL))
							continue;						

						String type = getBqType(t,field);
						if ("RECORD".equals(type)) {
							List<TableFieldSchema> child = getTableFieldSchema(t.getElementType());
							//Create repeatable table field
							// [TODO] Handle non struct in array
							TableFieldSchema tfs = new TableFieldSchema()
									.setName(field.name())
									.setType(type)
									.setFields(child)
									.setMode("REPEATED");
							tableFieldSchemas.add(tfs);
						} else if (type != null) {
							TableFieldSchema tfs = new TableFieldSchema()
									.setName(field.name())
									.setType(type);
							tableFieldSchemas.add(tfs);
						}
					}
				} else
				{
					TableFieldSchema tfs = new TableFieldSchema()
							.setName(field.name())
							.setType(getBqType(field.schema(),field));
					tableFieldSchemas.add(tfs);
				}
			}
		}
		return tableFieldSchemas;
	}

	static String getBqType(Schema t, Schema.Field f) {
		if (t.getType().equals(Schema.Type.STRING))
			return "STRING";
		else if (t.getType().equals(Schema.Type.FLOAT) || t.getType().equals(Schema.Type.DOUBLE))
			return "FLOAT";
		else if (t.getType().equals(Schema.Type.LONG) &&
				f.schema().getLogicalType() != null &&
				f.schema().getLogicalType().getName().equals("timestamp-millis"))
			return "DATETIME";
		else if (t.getType().equals(Schema.Type.INT) &&
				f.schema().getLogicalType() != null
				&& f.schema().getLogicalType().getName().equals("date"))
			return "DATE";
		else if (t.getType().equals(Schema.Type.INT)) {
			return "INTEGER";
		} else if (t.getType().equals(Schema.Type.LONG)) {
			return "INT64";
		} else if (t.getType().equals(Schema.Type.BOOLEAN))
			return "BOOL";
		else if (t.getType().equals(Schema.Type.ARRAY)) {
			return "RECORD";
		} else {
			LOG.error("No BQ type found for:" + t.getType());
			return null;
		}
	}

	//	public static TableSchema getSchema(Schema schema) {
	//		TableSchema bqSchema = new TableSchema();
	//		bqSchema.setFields(new ArrayList<TableFieldSchema>() {
	//			{
	//				for(Field f : schema.getFields())
	//				{
	//					if(f.schema().getType().equals(Type.UNION))
	//					{
	//						List<Schema> typeList = f.schema().getTypes();
	//						for (Schema t : typeList)
	//						{
	//							if(t.getType().equals(Type.NULL))
	//								continue;
	//
	//							String recordType = getBqType(t,f);
	//							if(recordType!=null && recordType.equals("RECORD"))
	//							{
	//								Schema sc = f.schema().getElementType();
	//								TableSchema ts = getSchema(sc);
	//								bqSchema.set(f.name(), ts);
	//							} 
	//							
	//							add(new TableFieldSchema().setName(f.name()).setType(getBqType(t,f)));
	//						}
	//					} else
	//					{
	//						add(new TableFieldSchema().setName(f.name()).setType(getBqType(f.schema(),f)));
	//					}
	//				}	           
	//			}
	//		});
	//		
	//		return bqSchema;
	//	}

	public static TableRow createTableRow(Schema schema, SpecificRecordBase msg) {

		// LOG.info("msg is:"+msg);
		TableRow bqrow = new TableRow();

		for(Field f : schema.getFields())
		{
			if(msg.get(f.name()) == null)
				continue;

			if(msg.get(f.name()) instanceof org.joda.time.DateTime)
			{
				//				LOG.info("f.name is:"+f.name());
				bqrow.set(f.name(), Utils.dateMsFormatter.print((DateTime)msg.get(f.name())));
			} 
			else if(msg.get(f.name()) instanceof List)
			{
				List<TableRow> deptListRow = new ArrayList<TableRow>();
				for(Object obj : (List)msg.get(f.name()))
				{
					TableRow deptr = createTableRow(f.schema(), (SpecificRecordBase)obj);
					deptListRow.add(deptr);
				}
										
//				List<EmpDept> deptList = (List<EmpDept>) msg.get(f.name());
//				for(EmpDept dept:deptList)
//				{
//					TableRow deptr = new TableRow()
//							.set("deptno", dept.deptno.toString())
//							.set("joindate", dept.joindate.toString());
//					deptListRow.add(deptr);
//				}
				bqrow.set(f.name(), deptListRow);				
			} else
			{
				//				LOG.info("default f.name is:"+f.name());
				bqrow.set(f.name(), msg.get(f.name()).toString());
			}
		}
		return bqrow;
	}
}
