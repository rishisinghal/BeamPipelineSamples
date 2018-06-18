package com.sample.beam.devices;

import com.sample.beam.df.shared.DeviceTelemetry;

public abstract class Device {

	public abstract DeviceTelemetry getTelemetryData(String dataPacket);
}
