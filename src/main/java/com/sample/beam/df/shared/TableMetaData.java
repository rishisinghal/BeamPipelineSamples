package com.sample.beam.df.shared;

import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.util.Objects;

@DefaultCoder(AvroCoder.class)
public class TableMetaData {
    @Nullable
    private String project;
    @Nullable
    private String dataset;
    @Nullable
    private String table;
    @Nullable
    private String partitioningKey;
    @Nullable
    private String portfolioKey;
    @Nullable
    private String appKey;
    @Nullable
    private String type;
    @Nullable
    private String event;

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public String getDataset() {
        return dataset;
    }

    public void setDataset(String dataset) {
        this.dataset = dataset;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    @Override
    public String toString() {
        return "TableMetaData{" +
                "project='" + project + '\'' +
                ", dataset='" + dataset + '\'' +
                ", table='" + table + '\'' +
                ", partitioningKey='" + partitioningKey + '\'' +
                ", portfolioKey='" + portfolioKey + '\'' +
                ", appKey='" + appKey + '\'' +
                ", type='" + type + '\'' +
                ", event='" + event + '\'' +
                '}';
    }

    public String getPartitioningKey() {
        return partitioningKey;
    }

    public void setPartitioningKey(String partitioningKey) {
        this.partitioningKey = partitioningKey;
    }

    public String getPortfolioKey() {
        return portfolioKey;
    }

    public void setPortfolioKey(String portfolioKey) {
        this.portfolioKey = portfolioKey;
    }

    public String getAppKey() {
        return appKey;
    }

    public void setAppKey(String appKey) {
        this.appKey = appKey;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableMetaData that = (TableMetaData) o;
        return Objects.equals(project, that.project) &&
                Objects.equals(dataset, that.dataset) &&
                Objects.equals(table, that.table) &&
                Objects.equals(partitioningKey, that.partitioningKey) &&
                Objects.equals(portfolioKey, that.portfolioKey) &&
                Objects.equals(appKey, that.appKey) &&
                Objects.equals(type, that.type) &&
                Objects.equals(event, that.event);
    }

    @Override
    public int hashCode() {
        return Objects.hash(project, dataset, table, partitioningKey, portfolioKey, appKey, type, event);
    }
}
