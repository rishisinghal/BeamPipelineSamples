package com.sample.beam.df.utils;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.sample.beam.df.shared.TableMetaData;

public class TableRowWithSchema {
    private final TableSchema tableSchema;
    private final TableRow tableRow;
    private final TableMetaData tableMetaData;

    private TableRowWithSchema() {
        tableSchema = null;
        tableRow = null;
        tableMetaData = null;
    }

    private TableRowWithSchema(
            TableSchema tableSchema,
            TableRow tableRow, TableMetaData tableMetaData) {
        this.tableSchema = tableSchema;
        this.tableRow = tableRow;
        this.tableMetaData = tableMetaData;
    }

    public TableSchema getTableSchema() {
        return tableSchema;
    }

    public TableRow getTableRow() {
        return tableRow;
    }

    public TableMetaData getTableMetaData() {
        return tableMetaData;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return "TableRowWithSchema{" +
                "tableSchema=" + tableSchema +
                ", tableRow=" + tableRow +
                ", tableMetaData=" + tableMetaData +
                '}';
    }

    public Builder toBuilder() {
        return new Builder(this);
    }

    public static final class Builder {
        private TableMetaData tableMetaData;
        private TableSchema tableSchema;
        private TableRow tableRow;

        Builder() {
        }

        private Builder(TableRowWithSchema tableRowWithSchema) {
            this.tableMetaData = tableRowWithSchema.tableMetaData;
            this.tableRow = tableRowWithSchema.tableRow;
            this.tableSchema = tableRowWithSchema.tableSchema;
        }

        public Builder withTableMetaData(TableMetaData tableMetaData) {
            if (tableMetaData == null) {
                throw new NullPointerException("Null table meta data");
            }
            this.tableMetaData = tableMetaData;
            return this;
        }

        public Builder withSchema(TableSchema tableSchema) {
            if (tableSchema == null) {
                throw new NullPointerException("Null table schema");
            }
            this.tableSchema = tableSchema;
            return this;
        }

        public Builder withTableRow(TableRow tableRow) {
            if (tableRow == null) {
                throw new NullPointerException("Null table row");
            }
            this.tableRow = tableRow;
            return this;
        }

        public TableRowWithSchema build() {
            String missing = "";
            if (this.tableMetaData == null) {
                missing += " tableMetaData";
            }
            if (this.tableSchema == null) {
                missing += " tableSchema";
            }
            if (this.tableRow == null) {
                missing += " tableRow";
            }
            if (!missing.isEmpty()) {
                throw new IllegalStateException("Missing required properties:" + missing);
            }
            return new TableRowWithSchema(
                    this.tableSchema,
                    this.tableRow,
                    this.tableMetaData);
        }

    }
}
