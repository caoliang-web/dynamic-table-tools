package com.selectdb.dynamic;


import com.selectdb.dynamic.doris.DataModel;
import com.selectdb.dynamic.util.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableSchema {
    public static final String DORIS_TABLE_REGEX = "^[a-zA-Z][a-zA-Z0-9-_]*$";
    private String database;
    private String table;
    private String tableComment;
    private Map<String, FieldSchema> fields;
    private List<String> keys = new ArrayList<>();
    private DataModel model = DataModel.DUPLICATE;
    private List<String> distributeKeys = new ArrayList<>();
    private Map<String, String> properties = new HashMap<>();
    private Integer tableBuckets;

    // Currently only supports auto partition, eg: DATE_TRUNC(column,interval)
    private Tuple2<String, String> partitionInfo;

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    public String getTableComment() {
        return tableComment;
    }

    public Map<String, FieldSchema> getFields() {
        return fields;
    }

    public List<String> getKeys() {
        return keys;
    }

    public DataModel getModel() {
        return model;
    }

    public List<String> getDistributeKeys() {
        return distributeKeys;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public void setTableComment(String tableComment) {
        this.tableComment = tableComment;
    }

    public void setFields(Map<String, FieldSchema> fields) {
        this.fields = fields;
    }

    public void setKeys(List<String> keys) {
        this.keys = keys;
    }

    public void setModel(DataModel model) {
        this.model = model;
    }

    public void setDistributeKeys(List<String> distributeKeys) {
        this.distributeKeys = distributeKeys;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public void setTableBuckets(Integer tableBuckets) {
        this.tableBuckets = tableBuckets;
    }

    public Integer getTableBuckets() {
        return tableBuckets;
    }

    public Tuple2<String, String> getPartitionInfo() {
        return partitionInfo;
    }

    public void setPartitionInfo(Tuple2<String, String> partitionInfo) {
        this.partitionInfo = partitionInfo;
    }

    @Override
    public String toString() {
        return "TableSchema{"
                + "database='"
                + database
                + '\''
                + ", table='"
                + table
                + '\''
                + ", tableComment='"
                + tableComment
                + '\''
                + ", fields="
                + fields
                + ", keys="
                + String.join(",", keys)
                + ", model="
                + model.name()
                + ", distributeKeys="
                + String.join(",", distributeKeys)
                + ", properties="
                + properties
                + ", tableBuckets="
                + tableBuckets
                + '}';
    }
}
