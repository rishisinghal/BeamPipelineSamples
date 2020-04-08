package com.sample.beam.df.utils;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.ValueProvider;

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
	
//  @Description("BigQuery Dataset ID")
//  @Default.String("employee")    
//  ValueProvider<String> getBQDatasetId();
//  void setBQDatasetId(ValueProvider<String> query);
//  
//  @Description("BigQuery Table Name")
//  @Default.String("empdetails")    
//  ValueProvider<String> getBQTableName();
//  void setBQTableName(ValueProvider<String> query);
    
    
}
