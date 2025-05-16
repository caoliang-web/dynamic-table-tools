package com.selectdb.dynamic;

import com.alibaba.fastjson.JSON;
import com.selectdb.dynamic.doris.DorisSystem;
import com.selectdb.dynamic.mysql.MysqlDatabaseSync;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.*;

public class dynamicTools {

    private static final Logger LOG = LoggerFactory.getLogger(dynamicTools.class);

    private static final String MYSQL_SYNC_DATABASE = "mysql";
    private static final String ORACLE_SYNC_DATABASE = "oracle";
    private static final String POSTGRES_SYNC_DATABASE = "postgres";
    private static final String SQLSERVER_SYNC_DATABASE = "sqlserver";
    private static final String MONGODB_SYNC_DATABASE = "mongodb";
    private static final List<String> EMPTY_KEYS = Collections.singletonList("password");
    private static Map<String, String> CONF;


    public static void main(String[] args) throws Exception {

        LOG.info("Input args: " + Arrays.asList(args) + ".\n");
        String properString = IOUtils.toString(new FileInputStream(args[0]), "UTF-8");
        CONF = load(properString);
        LOG.info("config is: " + JSON.toJSONString(CONF));

        switch (CONF.get(Constants.SOURCE_TYPE)) {
            case MYSQL_SYNC_DATABASE:
                createMySQLSyncDatabase(CONF);
                break;
            case ORACLE_SYNC_DATABASE:
                break;
            case POSTGRES_SYNC_DATABASE:
                break;
            case SQLSERVER_SYNC_DATABASE:
                break;
            case MONGODB_SYNC_DATABASE:
                break;
            default:
                LOG.error("Unknown operation " + CONF.get("type"));
                System.exit(1);
        }

    }

    private static void createMySQLSyncDatabase(Map<String, String> conf) throws Exception {
        DatabaseSync databaseSync = new MysqlDatabaseSync();
        syncDatabase(conf, databaseSync);
    }


    private static void syncDatabase(Map<String, String> config, DatabaseSync databaseSync) throws Exception {
        String database = config.get(Constants.TARGET_DATABASE);
        String tablePrefix = config.get(Constants.TABLE_PREFIX);
        String tableSuffix = config.get(Constants.TABLE_SUFFIX);
        String includingTables = config.get(Constants.SOURCE_INCLUDING_TABLE);
        String excludingTables = config.get(Constants.SOURCE_EXCLUDING_TABLE);
        Map<String, String> tableConfMap = JSON.parseObject(config.get(Constants.TABLE_CONF), Map.class);

        databaseSync
                .setDatabase(database)
                .setConfig(config)
                .setTablePrefix(tablePrefix)
                .setTableSuffix(tableSuffix)
                .setIncludingTables(includingTables)
                .setExcludingTables(excludingTables)
                .setTableConfig(tableConfMap)
                .create()
                .build();
    }


    public static Map<String, String> load(String propertiesString) throws IOException {
        Properties properties = new Properties();
        properties.load(new StringReader(propertiesString));
        return new HashMap<>((Map) properties);
    }


}
