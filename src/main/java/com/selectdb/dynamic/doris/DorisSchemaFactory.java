package com.selectdb.dynamic.doris;


import com.selectdb.dynamic.exception.CreateTableException;
import com.selectdb.dynamic.FieldSchema;
import com.selectdb.dynamic.util.Preconditions;
import com.selectdb.dynamic.SourceSchema;
import com.selectdb.dynamic.TableSchema;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.ObjectUtils;

import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


public class DorisSchemaFactory {

    public static TableSchema createTableSchema(
            String database,
            String table,
            Map<String, FieldSchema> columnFields,
            List<String> pkKeys,
            DorisTableConfig dorisTableConfig,
            String tableComment) {
        TableSchema tableSchema = new TableSchema();
        tableSchema.setDatabase(database);
        tableSchema.setTable(table);
        tableSchema.setModel(
                CollectionUtils.isEmpty(pkKeys) ? DataModel.DUPLICATE : DataModel.UNIQUE);
        tableSchema.setFields(columnFields);
        tableSchema.setKeys(buildKeys(pkKeys, columnFields));
        tableSchema.setTableComment(tableComment);
        tableSchema.setDistributeKeys(buildDistributeKeys(pkKeys, columnFields));
        if (Objects.nonNull(dorisTableConfig)) {
            tableSchema.setProperties(dorisTableConfig.getTableProperties());
            tableSchema.setTableBuckets(
                    parseTableSchemaBuckets(dorisTableConfig.getTableBuckets(), table));
            if (ObjectUtils.isNotEmpty(dorisTableConfig.getTablePartitions())
                    && dorisTableConfig.getTablePartitions().containsKey(table)) {
                tableSchema.setPartitionInfo(dorisTableConfig.getTablePartitions().get(table));
            }
        }
        return tableSchema;
    }

    private static List<String> buildDistributeKeys(
            List<String> primaryKeys, Map<String, FieldSchema> fields) {
        return buildKeys(primaryKeys, fields);
    }

    /**
     * Theoretically, the duplicate table of doris does not need to distinguish the key column, but
     * in the actual table creation statement, the key column will be automatically added. So if it
     * is a duplicate table, primaryKeys is empty, and we uniformly take the first field as the key.
     */
    private static List<String> buildKeys(
            List<String> primaryKeys, Map<String, FieldSchema> fields) {
        if (CollectionUtils.isNotEmpty(primaryKeys)) {
            return primaryKeys;
        }
        if (!fields.isEmpty()) {
            Map.Entry<String, FieldSchema> firstField = fields.entrySet().iterator().next();
            return Collections.singletonList(firstField.getKey());
        }
        return new ArrayList<>();
    }


    public static Integer parseTableSchemaBuckets(
            Map<String, Integer> tableBucketsMap, String tableName) {
        if (MapUtils.isNotEmpty(tableBucketsMap)) {
            // Firstly, if the table name is in the table-buckets map, set the buckets of the table.
            if (tableBucketsMap.containsKey(tableName)) {
                return tableBucketsMap.get(tableName);
            }
            // Secondly, iterate over the map to find a corresponding regular expression match.
            for (Map.Entry<String, Integer> entry : tableBucketsMap.entrySet()) {
                Pattern pattern = Pattern.compile(entry.getKey());
                if (pattern.matcher(tableName).matches()) {
                    return entry.getValue();
                }
            }
        }
        return null;
    }

    public static String generateCreateTableDDL(TableSchema schema) {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
        sb.append(identifier(schema.getDatabase()))
                .append(".")
                .append(identifier(schema.getTable()))
                .append("(");

        Map<String, FieldSchema> fields = schema.getFields();
        List<String> keys = schema.getKeys();
        // append keys
        for (String key : keys) {
            if (!fields.containsKey(key)) {
                throw new CreateTableException("key " + key + " not found in column list");
            }
            FieldSchema field = fields.get(key);
            buildColumn(sb, field, true, false);
        }

        // append partition column, auto partition column must be in keys
        if (schema.getPartitionInfo() != null) {
            String partitionCol = schema.getPartitionInfo().f0;
            FieldSchema field = fields.get(partitionCol);
            buildColumn(sb, field, true, true);
        }

        // append values
        for (Map.Entry<String, FieldSchema> entry : fields.entrySet()) {
            // skip key column
            if (keys.contains(entry.getKey())) {
                continue;
            }
            // skip partition column
            if (schema.getPartitionInfo() != null
                    && entry.getKey().equals(schema.getPartitionInfo().f0)) {
                continue;
            }
            FieldSchema field = entry.getValue();
            buildColumn(sb, field, false, false);
        }
        sb = sb.deleteCharAt(sb.length() - 1);
        sb.append(" ) ");
        // append uniq model
        if (DataModel.UNIQUE.equals(schema.getModel())) {
            sb.append(schema.getModel().name())
                    .append(" KEY(")
                    .append(String.join(",", identifier(schema.getKeys())));

            if (schema.getPartitionInfo() != null) {
                sb.append(",").append(identifier(schema.getPartitionInfo().f0));
            }

            sb.append(")");
        }

        // append table comment
        if (!Preconditions.isNullOrWhitespaceOnly(schema.getTableComment())) {
            sb.append(" COMMENT '").append(quoteComment(schema.getTableComment())).append("' ");
        }

        // append partition info if exists
        if (schema.getPartitionInfo() != null) {
            sb.append(" AUTO PARTITION BY RANGE ")
                    .append(
                            String.format(
                                    "(date_trunc(`%s`, '%s'))",
                                    schema.getPartitionInfo().f0, schema.getPartitionInfo().f1))
                    .append("()");
        }

        // append distribute key
        sb.append(" DISTRIBUTED BY HASH(")
                .append(String.join(",", identifier(schema.getDistributeKeys())))
                .append(")");

        Map<String, String> properties = schema.getProperties();
        if (schema.getTableBuckets() != null) {

            int bucketsNum = schema.getTableBuckets();
            if (bucketsNum <= 0) {
                throw new CreateTableException("The number of buckets must be positive.");
            }
            sb.append(" BUCKETS ").append(bucketsNum);
        } else {
            sb.append(" BUCKETS AUTO ");
        }

        // append properties
        int index = 0;
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (index == 0) {
                sb.append(" PROPERTIES (");
            }
            if (index > 0) {
                sb.append(",");
            }
            sb.append(quoteProperties(entry.getKey()))
                    .append("=")
                    .append(quoteProperties(entry.getValue()));
            index++;

            if (index == (schema.getProperties().size())) {
                sb.append(")");
            }
        }
        return sb.toString();
    }

    private static void buildColumn(
            StringBuilder sql, FieldSchema field, boolean isKey, boolean autoPartitionCol) {
        String fieldType = field.getTypeString();
        if (isKey && DorisType.STRING.equals(fieldType)) {
            fieldType = String.format("%s(%s)", DorisType.VARCHAR, 65533);
        }
        sql.append(identifier(field.getName())).append(" ").append(fieldType);

        // auto partition need set partition-column not null
        if (autoPartitionCol) {
            sql.append(" NOT NULL ");
        }

        if (field.getDefaultValue() != null) {
            sql.append(" DEFAULT " + quoteDefaultValue(field.getDefaultValue()));
        }
        sql.append(" COMMENT '").append(quoteComment(field.getComment())).append("',");
    }

    private static String quoteProperties(String name) {
        return "'" + name + "'";
    }

    private static List<String> identifier(List<String> names) {
        return names.stream().map(DorisSchemaFactory::identifier).collect(Collectors.toList());
    }

    public static String identifier(String name) {
        if (name.startsWith("`") && name.endsWith("`")) {
            return name;
        }
        return "`" + name + "`";
    }

    public static String quoteDefaultValue(String defaultValue) {
        // DEFAULT current_timestamp or null not need quote
        if (defaultValue.equalsIgnoreCase("current_timestamp")
                || defaultValue.equalsIgnoreCase("null")) {
            return defaultValue;
        }

        return "'" + defaultValue + "'";
    }

    public static String quoteComment(String comment) {
        if (comment == null) {
            return "";
        } else {
            return comment.replaceAll("'", "\\\\'");
        }
    }

    public static String quoteTableIdentifier(String tableIdentifier) {
        String[] dbTable = tableIdentifier.split("\\.");
        Preconditions.checkArgument(dbTable.length == 2);
        return identifier(dbTable[0]) + "." + identifier(dbTable[1]);
    }


}
