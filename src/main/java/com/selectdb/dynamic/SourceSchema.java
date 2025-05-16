package com.selectdb.dynamic;



import com.selectdb.dynamic.doris.DataModel;
import com.selectdb.dynamic.util.Preconditions;

import java.util.*;

public abstract class SourceSchema {
    protected final String databaseName;
    protected final String schemaName;
    protected final String tableName;
    protected final String tableComment;
    protected LinkedHashMap<String, FieldSchema> fields;
    public List<String> primaryKeys;
    public List<String> uniqueIndexs;
    public DataModel model = DataModel.UNIQUE;

    public SourceSchema(
            String databaseName, String schemaName, String tableName, String tableComment)
             {
        this.databaseName = databaseName;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.tableComment = tableComment;
    }

    public abstract String convertToDorisType(String fieldType, Integer precision, Integer scale);


    public String getTableIdentifier() {
        return getString(databaseName, schemaName, tableName);
    }

    public static String getString(String databaseName, String schemaName, String tableName) {
        StringJoiner identifier = new StringJoiner(".");
        if (!Preconditions.isNullOrWhitespaceOnly(databaseName)) {
            identifier.add(databaseName);
        }
        if (!Preconditions.isNullOrWhitespaceOnly(schemaName)) {
            identifier.add(schemaName);
        }
        if (!Preconditions.isNullOrWhitespaceOnly(tableName)) {
            identifier.add(tableName);
        }

        return identifier.toString();
    }

    @Deprecated
    public TableSchema convertTableSchema(Map<String, String> tableProps) {
        TableSchema tableSchema = new TableSchema();
        tableSchema.setModel(this.model);
        tableSchema.setFields(this.fields);
        tableSchema.setKeys(buildKeys());
        tableSchema.setTableComment(this.tableComment);
        tableSchema.setDistributeKeys(buildDistributeKeys());
        tableSchema.setProperties(tableProps);
        return tableSchema;
    }

    private List<String> buildKeys() {
        return buildDistributeKeys();
    }

    private List<String> buildDistributeKeys() {
        if (!this.primaryKeys.isEmpty()) {
            return primaryKeys;
        }
        if (!this.fields.isEmpty()) {
            Map.Entry<String, FieldSchema> firstField = this.fields.entrySet().iterator().next();
            return Collections.singletonList(firstField.getKey());
        }
        return new ArrayList<>();
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public Map<String, FieldSchema> getFields() {
        return fields;
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public List<String> getUniqueIndexs() {
        return uniqueIndexs;
    }

    public String getTableComment() {
        return tableComment;
    }

    public DataModel getModel() {
        return model;
    }

    public void setModel(DataModel model) {
        this.model = model;
    }


}
