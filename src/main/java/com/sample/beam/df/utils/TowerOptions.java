package com.sample.beam.df.utils;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

public interface TowerOptions extends DataflowPipelineOptions {
	
	// BQ options 
    @Description("BigQuery Dataset ID")
    @Default.String("tower")    
    String getBQDatasetId();
    void setBQDatasetId(String s);
    
    @Description("BigQuery Table Name")
    @Default.String("device_state")    
    String getBQTableName();
    void setBQTableName(String s);
    
    // BT options
    @Description("BigTable Instance ID")
    @Default.String("tower-instance")    
    String getBTInstanceId();
    void setBTInstanceId(String s);
    
    @Description("BigTable Table Telemetry")
    @Default.String("device_telemetry")    
    String getBTTelemetryTableId();
    void setBTTelemetryTableId(String s);
    
    @Description("BigTable Table State")
    @Default.String("device_state")    
    String getBTStateTableId();
    void setBTStateTableId(String s);
    
    @Description("BigTable Column Family")
    @Default.String("device_fm")    
    String getBTColFamily();
    void setBTColFamily(String s);
}
