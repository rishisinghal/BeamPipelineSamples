package com.sample.beam.df.process;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema.Field;
import org.apache.avro.specific.SpecificRecord;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.bigtable.admin.v2.ColumnFamily;
import com.google.bigtable.admin.v2.CreateTableRequest;
import com.google.bigtable.admin.v2.GetTableRequest;
import com.google.bigtable.admin.v2.Table;
import com.google.bigtable.v2.Mutation;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.CredentialOptions;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.BigtableTableAdminClient;
import com.google.protobuf.ByteString;
import com.sample.beam.df.shared.DeviceTelemetry;
import com.sample.beam.df.utils.TowerOptions;
import com.sample.beam.df.utils.Utils;

public class BigTableTelemetryProcess extends DoFn<SpecificRecord, KV<ByteString, Iterable<Mutation>>> {

	private static final long serialVersionUID = 1462827258689031685L;
	private static final Logger LOG = LoggerFactory.getLogger(BigTableTelemetryProcess.class);
	private String btColFamily;
	
	public BigTableTelemetryProcess(String btColFamily) {
		this.btColFamily = btColFamily;
	}

	@ProcessElement
	public void processElement(DoFn<SpecificRecord, KV<ByteString, Iterable<Mutation>>>.ProcessContext c) throws Exception {

		DeviceTelemetry msg = (DeviceTelemetry) c.element();

		long ts = msg.getTimestamp().getMillis();
		ts -= (ts % 1000);
		String key = ts+"";

		List<Mutation> mutations = new ArrayList<>();

		for(Field f : DeviceTelemetry.getClassSchema().getFields())
		{
			// LOG.info("name:"+f.name()+" Val:"+msg.get(f.name()));			
			if(msg.get(f.name()) instanceof org.joda.time.DateTime)
				addCell(mutations, f.name(), Utils.dateMsFormatter.print(msg.getTimestamp()), ts);
			else
			{
				addCell(mutations, f.name(), msg.get(f.name()).toString(), ts);
			}
		}

		c.output(KV.of(ByteString.copyFromUtf8(key), mutations));
	}

	private void addCell(List<Mutation> mutations, String cellName, String cellValue, long ts) {
		if (cellValue.length() > 0) {
			ByteString value = ByteString.copyFromUtf8(cellValue);
			ByteString colname = ByteString.copyFromUtf8(cellName);
			Mutation m = Mutation.newBuilder().setSetCell(Mutation.SetCell.newBuilder() 					
					.setValue(value)//
					.setFamilyName(btColFamily)//
					.setColumnQualifier(colname)//
					.setTimestampMicros(ts) //
					).build();
			mutations.add(m);
		}
	}

	public void createEmptyTable(BigtableOptions.Builder optionsBuilder, TowerOptions options) {

		Table.Builder tableBuilder = Table.newBuilder();
		ColumnFamily cf = ColumnFamily.newBuilder().build();
		tableBuilder.putColumnFamilies(options.getBTColFamily(), cf);

		try (BigtableSession session = new BigtableSession(optionsBuilder
				.setCredentialOptions(CredentialOptions.credential(options.as(GcpOptions.class).getGcpCredential())).build())) {
			BigtableTableAdminClient tableAdminClient = session.getTableAdminClient();

			try {
				// if get fails, then create
				String tableName = getTableName(options);
				LOG.info("BigTable if present is:"+tableName);
				GetTableRequest.Builder getTableRequestBuilder = GetTableRequest.newBuilder().setName(tableName);
				tableAdminClient.getTable(getTableRequestBuilder.build());
			} catch (Exception e) {
				LOG.error("GET table exception:"+e.getMessage(), e);

				CreateTableRequest.Builder createTableRequestBuilder = //
						CreateTableRequest.newBuilder().setParent(options.getBTInstanceId()) //
						.setTableId(options.getBTTelemetryTableId()).setTable(tableBuilder.build());
				tableAdminClient.createTable(createTableRequestBuilder.build());
			}

		} catch (IOException e) {
			LOG.error("create table exception:"+e.getMessage(), e);
			throw new RuntimeException(e);
		}
	}

	private String getTableName(TowerOptions options) {
		return String.format("%s/tables/%s", options.getBTInstanceId(), options.getBTTelemetryTableId());
	}
}
