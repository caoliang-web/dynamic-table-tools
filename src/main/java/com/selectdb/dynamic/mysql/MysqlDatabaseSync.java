package com.selectdb.dynamic.mysql;


import com.selectdb.dynamic.Constants;
import com.selectdb.dynamic.DatabaseSync;
import com.selectdb.dynamic.SourceSchema;
import com.selectdb.dynamic.doris.DataModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;



public class MysqlDatabaseSync extends DatabaseSync {
    private static final Logger LOG = LoggerFactory.getLogger(MysqlDatabaseSync.class);
    private static final String JDBC_URL = "jdbc:mysql://%s:%d?useInformationSchema=true";

    public MysqlDatabaseSync() throws SQLException {
        super();
    }

    @Override
    public void registerDriver() throws SQLException {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException ex) {
            LOG.warn(
                    "can not found class com.mysql.cj.jdbc.Driver, use class com.mysql.jdbc.Driver");
            try {
                Class.forName("com.mysql.jdbc.Driver");
            } catch (Exception e) {
                throw new SQLException(
                        "No suitable driver found, can not found class com.mysql.cj.jdbc.Driver and com.mysql.jdbc.Driver");
            }
        }
    }

    @Override
    public Connection getConnection() throws SQLException {

        Properties jdbcProperties = getJdbcProperties();
        String jdbcUrlTemplate = getJdbcUrlTemplate(JDBC_URL, jdbcProperties);
        String jdbcUrl =
                String.format(
                        jdbcUrlTemplate,
                        config.get(Constants.SOURCE_IP),
                        Integer.parseInt(config.get(Constants.SOURCE_PORT)));

        return DriverManager.getConnection(
                jdbcUrl,
                config.get(Constants.SOURCE_USER),
                config.get(Constants.SOURCE_PASSWORD));
    }

    @Override
    public List<SourceSchema> getSchemaList() throws Exception {
        String databaseName = config.get(Constants.SOURCE_DATABASE);

        List<SourceSchema> schemaList = new ArrayList<>();
        try (Connection conn = getConnection()) {
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet catalogs = metaData.getCatalogs()) {
                while (catalogs.next()) {
                    String tableCatalog = catalogs.getString("TABLE_CAT");
                    if (tableCatalog.matches(databaseName)) {
                        try (ResultSet tables =
                                     metaData.getTables(
                                             tableCatalog, null, "%", new String[]{"TABLE"})) {
                            while (tables.next()) {
                                String tableName = tables.getString("TABLE_NAME");
                                String tableComment = tables.getString("REMARKS");
                                if (!isSyncNeeded(tableName)) {
                                    continue;
                                }
                                SourceSchema sourceSchema =
                                        new MysqlSchema(
                                                metaData, tableCatalog, tableName, tableComment);
                                sourceSchema.setModel(
                                        !sourceSchema.primaryKeys.isEmpty()
                                                ? DataModel.UNIQUE
                                                : DataModel.DUPLICATE);
                                schemaList.add(sourceSchema);
                            }
                        }
                    }
                }
            }
        }
        return schemaList;
    }

    @Override
    public String getTableListPrefix() {
        return config.get(Constants.SOURCE_DATABASE);
    }
}
