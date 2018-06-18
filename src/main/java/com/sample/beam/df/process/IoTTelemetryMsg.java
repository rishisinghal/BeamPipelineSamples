package com.sample.beam.df.process;

import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sample.beam.devices.Device;
import com.sample.beam.devices.DeviceFactory;
import com.sample.beam.df.shared.DeviceTelemetry;

public class IoTTelemetryMsg extends DoFn<String, DeviceTelemetry> {

	private static final long serialVersionUID = 1462827258689031685L;
	private static final Logger LOG = LoggerFactory.getLogger(IoTTelemetryMsg.class);

	@ProcessElement
	public void processElement(DoFn<String, DeviceTelemetry>.ProcessContext c) throws Exception {
		String dataPacket = c.element();	

		DeviceFactory df = new DeviceFactory();
		Device dev = df.getDevice("tcp");

		try {
			DeviceTelemetry dt = dev.getTelemetryData(dataPacket);
			LOG.info("DeviceTelemetry packet is:"+dt.toString());
			c.output(dt);
		} catch(Exception e)
		{
			LOG.error("Exception in processing packet:"+e.getMessage(), e);
		}
	}
}
