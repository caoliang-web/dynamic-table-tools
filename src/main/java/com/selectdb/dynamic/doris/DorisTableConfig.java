package com.selectdb.dynamic.doris;

import com.selectdb.dynamic.util.Tuple2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class DorisTableConfig implements Serializable {
    public static final String LIGHT_SCHEMA_CHANGE = "light_schema_change";
    // PROPERTIES parameter in doris table creation statement. such as: replication_num=1.
    public static final String REPLICATION_NUM = "replication_num";
    public static final String TABLE_BUCKETS = "table-buckets";
    public static final String TABLE_PARTITIONS = "table-partitions";
    public static final String CONVERT_UNIQ_TO_PK = "convert-uniq-to-pk";

    private final Map<String, String> tableProperties;
    // The specific parameters extracted from --table-conf need to be parsed and integrated into the
    // doris table creation statement. such as: table-buckets="tbl1:10,tbl2:20,a.*:30,b.*:40,.*:50".
    private Map<String, Integer> tableBuckets;
    // table:partitionColumn:interval
    private Map<String, Tuple2<String, String>> tablePartitions;
    // uniq index to primary key
    private boolean convertUniqToPk = false;

    // Only for testing
    public DorisTableConfig() {
        tableProperties = new HashMap<>();
        tableBuckets = new HashMap<>();
    }

    public DorisTableConfig(Map<String, String> tableConfig) {
        if (Objects.isNull(tableConfig)) {
            tableConfig = new HashMap<>();
        }
        // default enable light schema change
        if (!tableConfig.containsKey(LIGHT_SCHEMA_CHANGE)) {
            tableConfig.put(LIGHT_SCHEMA_CHANGE, Boolean.toString(true));
        }
        if (tableConfig.containsKey(TABLE_BUCKETS)) {
            this.tableBuckets = buildTableBucketMap(tableConfig.get(TABLE_BUCKETS));
            tableConfig.remove(TABLE_BUCKETS);
        }
        if (tableConfig.containsKey(TABLE_PARTITIONS)) {
            this.tablePartitions = buildTablePartitionMap(tableConfig.get(TABLE_PARTITIONS));
            tableConfig.remove(TABLE_PARTITIONS);
        }

        if (tableConfig.containsKey(CONVERT_UNIQ_TO_PK)) {
            this.convertUniqToPk = Boolean.parseBoolean(tableConfig.get(CONVERT_UNIQ_TO_PK));
            tableConfig.remove(CONVERT_UNIQ_TO_PK);
        }

        tableProperties = tableConfig;
    }

    public Map<String, Integer> getTableBuckets() {
        return tableBuckets;
    }

    public Map<String, String> getTableProperties() {
        return tableProperties;
    }

    public Map<String, Tuple2<String, String>> getTablePartitions() {
        return tablePartitions;
    }

    public boolean isConvertUniqToPk() {
        return convertUniqToPk;
    }

    /**
     * Build table bucket Map.
     *
     * @param tableBuckets the string of tableBuckets, eg:student:10,student_info:20,student.*:30
     * @return The table name and buckets map. The key is table name, the value is buckets.
     */
    public Map<String, Integer> buildTableBucketMap(String tableBuckets) {
        Map<String, Integer> tableBucketsMap = new LinkedHashMap<>();
        String[] tableBucketsArray = tableBuckets.split(",");
        for (String tableBucket : tableBucketsArray) {
            String[] tableBucketArray = tableBucket.split(":");
            tableBucketsMap.put(
                    tableBucketArray[0].trim(), Integer.parseInt(tableBucketArray[1].trim()));
        }
        return tableBucketsMap;
    }

    /**
     * Build table partition Map.
     *
     * @param tablePartitions the string of tablePartitions,
     *     eg:tbl1:dt_column:month,tb2:dt_column:day
     * @return The table name and buckets map. The key is table name, the value is partition column
     *     and interval.
     */
    public Map<String, Tuple2<String, String>> buildTablePartitionMap(String tablePartitions) {
        Map<String, Tuple2<String, String>> tablePartitionMap = new LinkedHashMap<>();
        String[] tablePartitionArray = tablePartitions.split(",");
        for (String tablePartition : tablePartitionArray) {
            String[] tp = tablePartition.split(":");
            tablePartitionMap.put(tp[0].trim(), Tuple2.of(tp[1].trim(), tp[2].trim()));
        }
        return tablePartitionMap;
    }
}
