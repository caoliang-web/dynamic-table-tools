package com.selectdb.dynamic;


import com.selectdb.dynamic.connection.DorisConnectionOptions;
import com.selectdb.dynamic.doris.DorisSystem;
import com.selectdb.dynamic.doris.DorisTableConfig;
import com.selectdb.dynamic.oracle.OracleDatabaseSync;
import com.selectdb.dynamic.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Pattern;


public abstract class DatabaseSync {
    private static final Logger LOG = LoggerFactory.getLogger(DatabaseSync.class);

    public Map<String, String> config;

    private String database;

    private Pattern includingPattern;
    private Pattern excludingPattern;
    private DorisTableConfig dorisTableConfig;

    private boolean ignoreIncompatible;
    private TableNameConverter converter;


    private String includingTables;
    private String excludingTables;

    private String tablePrefix;
    private String tableSuffix;
    private final Map<String, String> tableMapping = new HashMap<>();

    public abstract void registerDriver() throws SQLException;

    public abstract Connection getConnection() throws SQLException;

    public abstract List<SourceSchema> getSchemaList() throws Exception;


    /**
     * Get the prefix of a specific tableList, for example, mysql is database, oracle is schema.
     */
    public abstract String getTableListPrefix();

    protected DatabaseSync() throws SQLException {
        registerDriver();
    }

    public DatabaseSync create() {
        this.includingPattern = includingTables == null ? null : Pattern.compile(includingTables);
        this.excludingPattern = excludingTables == null ? null : Pattern.compile(excludingTables);
        this.converter = new TableNameConverter(tablePrefix, tableSuffix);
        return this;
    }

    public void build() throws Exception {
        DorisConnectionOptions options = getDorisConnectionOptions();
        DorisSystem dorisSystem = new DorisSystem(options);

        List<SourceSchema> schemaList = getSchemaList();
        Preconditions.checkState(
                !schemaList.isEmpty(),
                "No tables to be synchronized. Please make sure whether the tables that need to be synchronized exist in the corresponding database or schema.");

        if (!Preconditions.isNullOrWhitespaceOnly(database)
                && !dorisSystem.databaseExists(database)) {
            LOG.info("database {} not exist, created", database);
            dorisSystem.createDatabase(database);
        }
        List<String> syncTables = new ArrayList<>();
        List<Tuple2<String, String>> dorisTables = new ArrayList<>();

        Set<String> targetDbSet = new HashSet<>();
        for (SourceSchema schema : schemaList) {
            syncTables.add(schema.getTableName());
            String targetDb = database;
            // Synchronize multiple databases using the src database name
            if (Preconditions.isNullOrWhitespaceOnly(targetDb)) {
                targetDb = schema.getDatabaseName();
                targetDbSet.add(targetDb);
            }
            if (Preconditions.isNullOrWhitespaceOnly(database)
                    && !dorisSystem.databaseExists(targetDb)) {
                LOG.info("database {} not exist, created", targetDb);
                dorisSystem.createDatabase(targetDb);
            }
            LOG.info("database {} exist", targetDb);
            String dorisTable = converter.convert(schema.getTableName());
            // Calculate the mapping relationship between upstream and downstream tables
            tableMapping.put(
                    schema.getTableIdentifier(), String.format("%s.%s", targetDb, dorisTable));
            DorisTableUtil.tryCreateTableIfAbsent(
                    dorisSystem,
                    targetDb,
                    dorisTable,
                    schema,
                    dorisTableConfig,
                    ignoreIncompatible);

            if (!dorisTables.contains(Tuple2.of(targetDb, dorisTable))) {
                dorisTables.add(Tuple2.of(targetDb, dorisTable));
            }
        }

        LOG.info("Create table finished.");

    }

    /**
     * @param targetDbSet The set of target databases.
     * @param dbTbl       The database-table tuple.
     * @return The UID of the DataStream.
     */
    public String getUidName(Set<String> targetDbSet, Tuple2<String, String> dbTbl) {
        String uidName;
        // Determine whether to proceed with multi-database synchronization.
        // if yes, the UID is composed of `dbname_tablename`, otherwise it is composed of
        // `tablename`.
        if (targetDbSet.size() > 1) {
            uidName = dbTbl.f0 + "_" + dbTbl.f1;
        } else {
            uidName = dbTbl.f1;
        }

        return uidName;
    }

    private DorisConnectionOptions getDorisConnectionOptions() {
        String user = config.get(Constants.TARGET_USER);
        String passwd = config.get(Constants.TARGET_PASSWORD);
        String jdbcUrl = config.get(Constants.TARGET_URL);
        Preconditions.checkNotNull(user, "username is empty in sink-conf");
        Preconditions.checkNotNull(jdbcUrl, "jdbcurl is empty in sink-conf");
        DorisConnectionOptions.DorisConnectionOptionsBuilder builder =
                new DorisConnectionOptions.DorisConnectionOptionsBuilder()
                        .withUsername(user)
                        .withPassword(passwd)
                        .withJdbcUrl(jdbcUrl);
        return builder.build();
    }


    /**
     * Filter table that need to be synchronized.
     */
    protected boolean isSyncNeeded(String tableName) {
        boolean sync = true;
        if (includingPattern != null) {
            sync = includingPattern.matcher(tableName).matches();
        }
        if (excludingPattern != null) {
            sync = sync && !excludingPattern.matcher(tableName).matches();
        }
        LOG.debug("table {} is synchronized? {}", tableName, sync);
        return sync;
    }


    /**
     * Get table buckets Map.
     *
     * @param tableBuckets the string of tableBuckets, eg:student:10,student_info:20,student.*:30
     * @return The table name and buckets map. The key is table name, the value is buckets.
     */
    @Deprecated
    public static Map<String, Integer> getTableBuckets(String tableBuckets) {
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
     * Set table schema buckets.
     *
     * @param tableBucketsMap The table name and buckets map. The key is table name, the value is
     *                        buckets.
     * @param dorisSchema     @{TableSchema}
     * @param dorisTable      the table name need to set buckets
     * @param tableHasSet     The buckets table is set
     */
    @Deprecated
    public void setTableSchemaBuckets(
            Map<String, Integer> tableBucketsMap,
            TableSchema dorisSchema,
            String dorisTable,
            Set<String> tableHasSet) {

        if (tableBucketsMap != null) {
            // Firstly, if the table name is in the table-buckets map, set the buckets of the table.
            if (tableBucketsMap.containsKey(dorisTable)) {
                dorisSchema.setTableBuckets(tableBucketsMap.get(dorisTable));
                tableHasSet.add(dorisTable);
                return;
            }
            // Secondly, iterate over the map to find a corresponding regular expression match,
            for (Map.Entry<String, Integer> entry : tableBucketsMap.entrySet()) {
                if (tableHasSet.contains(entry.getKey())) {
                    continue;
                }

                Pattern pattern = Pattern.compile(entry.getKey());
                if (pattern.matcher(dorisTable).matches()) {
                    dorisSchema.setTableBuckets(entry.getValue());
                    tableHasSet.add(dorisTable);
                    return;
                }
            }
        }
    }

    public Properties getJdbcProperties() {
        Properties jdbcProps = new Properties();
        for (Map.Entry<String, String> entry : config.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key.startsWith(JdbcUrlUtils.PROPERTIES_PREFIX)) {
                jdbcProps.put(key.substring(JdbcUrlUtils.PROPERTIES_PREFIX.length()), value);
            }
        }
        return jdbcProps;
    }

    public String getJdbcUrlTemplate(String initialJdbcUrl, Properties jdbcProperties) {
        StringBuilder jdbcUrlBuilder = new StringBuilder(initialJdbcUrl);
        jdbcProperties.forEach(
                (key, value) -> jdbcUrlBuilder.append("&").append(key).append("=").append(value));
        return jdbcUrlBuilder.toString();
    }


    public DatabaseSync setConfig(Map<String, String> config) {
        this.config = config;
        return this;
    }

    public DatabaseSync setDatabase(String database) {
        this.database = database;
        return this;
    }

    public DatabaseSync setIncludingTables(String includingTables) {
        this.includingTables = includingTables;
        return this;
    }

    public DatabaseSync setExcludingTables(String excludingTables) {
        this.excludingTables = excludingTables;
        return this;
    }


    public DatabaseSync setTableConfig(Map<String, String> tableConfig) {
        if (!CollectionUtil.isNullOrEmpty(tableConfig)) {
            this.dorisTableConfig = new DorisTableConfig(tableConfig);
        }
        return this;
    }

    public DatabaseSync setTablePrefix(String tablePrefix) {
        this.tablePrefix = tablePrefix;
        return this;
    }

    public DatabaseSync setTableSuffix(String tableSuffix) {
        this.tableSuffix = tableSuffix;
        return this;
    }
}
