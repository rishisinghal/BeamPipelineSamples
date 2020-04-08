package com.sample.beam.df.utils;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

public interface SpannerToBQOptions extends DataflowPipelineOptions {

    @Description("Spanner instance id")
    String getSpannerInstanceId();
    void setSpannerInstanceId(String instanceId);

    @Description("Spanner Database id")
    String getSpannerDatabaseId();
    void setSpannerDatabaseId(String databaseId);

    @Description("Spanner Table")
    String getSpannerTable();
    void setSpannerTable(String table);
    
    @Description("Spanner Table Columns")
    String getSpannerColumns();
    void setSpannerColumns(String col);

    @Description("BigQuery Dataset ID")
    String getBQDatasetId();
    void setBQDatasetId(String s);
    
    @Description("BigQuery Table Name")
    String getBQTableName();
    void setBQTableName(String s);
    
    @Description("BigQuery target")
    String getBQTarget();
    void setBQTarget(String s);
    
}
