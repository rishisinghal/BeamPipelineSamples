package com.sample.beam.df.shared;

import com.google.api.services.bigquery.model.TableSchema;

import java.util.Objects;

public class CustomDestination {
    private TableMetaData tableMetaData;
    private TableSchema tableSchema;

    public TableMetaData getTableMetaData() {
        return tableMetaData;
    }

    public void setTableMetaData(TableMetaData tableMetaData) {
        this.tableMetaData = tableMetaData;
    }

    public TableSchema getTableSchema() {
        return tableSchema;
    }

    public void setTableSchema(TableSchema tableSchema) {
        this.tableSchema = tableSchema;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CustomDestination that = (CustomDestination) o;
        return Objects.equals(tableMetaData, that.tableMetaData) &&
                Objects.equals(tableSchema, that.tableSchema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableMetaData, tableSchema);
    }

    @Override
    public String toString() {
        return "CustomDestination{" +
                "tableMetaData=" + tableMetaData +
                ", tableSchema=" + tableSchema +
                '}';
    }
}
