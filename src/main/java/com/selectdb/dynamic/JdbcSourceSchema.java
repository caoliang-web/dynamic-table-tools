package com.selectdb.dynamic;



import com.selectdb.dynamic.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * JdbcSourceSchema is a subclass of SourceSchema, used to build metadata about jdbc-related
 * databases.
 */
public abstract class JdbcSourceSchema extends SourceSchema {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcSourceSchema.class);

    public JdbcSourceSchema(
            DatabaseMetaData metaData,
            String databaseName,
            String schemaName,
            String tableName,
            String tableComment)
            throws Exception {
        super(databaseName, schemaName, tableName, tableComment);
        fields = getColumnInfo(metaData, databaseName, schemaName, tableName);
        primaryKeys = getPrimaryKeys(metaData, databaseName, schemaName, tableName);
        uniqueIndexs = getUniqIndex(metaData, databaseName, schemaName, tableName);
    }

    public LinkedHashMap<String, FieldSchema> getColumnInfo(
            DatabaseMetaData metaData, String databaseName, String schemaName, String tableName)
            throws SQLException {
        LinkedHashMap<String, FieldSchema> fields = new LinkedHashMap<>();
        LOG.debug("Starting to get column info for table: {}", tableName);
        try (ResultSet rs = metaData.getColumns(databaseName, schemaName, tableName, null)) {
            while (rs.next()) {
                String fieldName = rs.getString("COLUMN_NAME");
                String comment = rs.getString("REMARKS");
                String fieldType = rs.getString("TYPE_NAME");
                Integer precision = rs.getInt("COLUMN_SIZE");

                if (rs.wasNull()) {
                    precision = null;
                }
                Integer scale = rs.getInt("DECIMAL_DIGITS");
                if (rs.wasNull()) {
                    scale = null;
                }
                String dorisTypeStr = null;
                try {
                    dorisTypeStr = convertToDorisType(fieldType, precision, scale);
                } catch (UnsupportedOperationException e) {
                    throw new UnsupportedOperationException(e + " in table: " + tableName);
                }
                fields.put(fieldName, new FieldSchema(fieldName, dorisTypeStr, comment));
            }
        }
        Preconditions.checkArgument(!fields.isEmpty(), "The column info of {} is empty", tableName);
        LOG.debug("Successfully retrieved column info for table: {}", tableName);
        return fields;
    }

    public List<String> getPrimaryKeys(
            DatabaseMetaData metaData, String databaseName, String schemaName, String tableName)
            throws SQLException {
        List<String> primaryKeys = new ArrayList<>();
        try (ResultSet rs = metaData.getPrimaryKeys(databaseName, schemaName, tableName)) {
            while (rs.next()) {
                String fieldName = rs.getString("COLUMN_NAME");
                primaryKeys.add(fieldName);
            }
        }

        return primaryKeys;
    }

    /**
     * Get the unique index of the table If the primary key is empty but there is a uniq key, then
     * use the uniqkey instead of the primarykey
     */
    public List<String> getUniqIndex(
            DatabaseMetaData metaData, String databaseName, String schemaName, String tableName)
            throws SQLException {
        Map<String, List<String>> uniqIndexMap = new HashMap<>();
        String firstIndexName = null;
        try (ResultSet rs =
                metaData.getIndexInfo(databaseName, schemaName, tableName, true, true)) {
            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                String indexName = rs.getString("INDEX_NAME");
                if (firstIndexName == null) {
                    firstIndexName = indexName;
                }
                uniqIndexMap.computeIfAbsent(indexName, k -> new ArrayList<>()).add(columnName);
            }
        }
        if (!uniqIndexMap.isEmpty()) {
            // If there are multiple uniq indices, return one
            return uniqIndexMap.get(firstIndexName);
        }
        return new ArrayList<>();
    }

    public abstract String convertToDorisType(String fieldType, Integer precision, Integer scale);
}
