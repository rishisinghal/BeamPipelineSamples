package com.sample.beam.df.utils;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

public interface DatabaseOptions extends DataflowPipelineOptions {
	
	// BQ options 
    @Description("BigQuery Dataset ID")
    @Default.String("employee")    
    String getBQDatasetId();
    void setBQDatasetId(String s);
    
    @Description("BigQuery Table Name")
    @Default.String("empdetails")    
    String getBQTableName();
    void setBQTableName(String s);
}
