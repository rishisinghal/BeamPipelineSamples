package com.sample.beam.df.utils;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface FirestoreTypesPipelineOptions extends PipelineOptions {

    @Description("Target BigQuery Dataset")
    String getBigQueryDataset();
    void setBigQueryDataset(String value);

    @Description("Target BigQuery Table")
    String getTargetBQTable();
    void setTargetBQTable(String value);

    @Description("Source BigQuery Table")
    String getSourceBQTable();
    void setSourceBQTable(String value);
}

