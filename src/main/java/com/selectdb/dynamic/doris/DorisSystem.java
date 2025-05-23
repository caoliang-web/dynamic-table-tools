package com.selectdb.dynamic.doris;


import com.selectdb.dynamic.TableSchema;
import com.selectdb.dynamic.connection.DorisConnectionOptions;
import com.selectdb.dynamic.connection.JdbcConnectionProvider;
import com.selectdb.dynamic.connection.SimpleJdbcConnectionProvider;
import com.selectdb.dynamic.exception.CreateTableException;
import com.selectdb.dynamic.util.Preconditions;
import org.apache.commons.compress.utils.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.util.function.Predicate;

import static com.selectdb.dynamic.util.Preconditions.checkArgument;

/** Doris System Operate. */
public class DorisSystem implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DorisSystem.class);
    private final JdbcConnectionProvider jdbcConnectionProvider;
    private static final List<String> builtinDatabases =
            Collections.singletonList("information_schema");

    public DorisSystem(DorisConnectionOptions options) {
        this.jdbcConnectionProvider = new SimpleJdbcConnectionProvider(options);
    }

    public List<String> listDatabases() {
        return extractColumnValuesBySQL(
                "SELECT `SCHEMA_NAME` FROM `INFORMATION_SCHEMA`.`SCHEMATA`;",
                1,
                dbName -> !builtinDatabases.contains(dbName));
    }

    public boolean databaseExists(String database) {
        checkArgument(!Preconditions.isNullOrWhitespaceOnly(database));
        return listDatabases().contains(database);
    }

    public boolean createDatabase(String database) {
        execute(String.format("CREATE DATABASE IF NOT EXISTS %s", database));
        return true;
    }

    public boolean dropDatabase(String database) {
        execute(String.format("DROP DATABASE IF EXISTS %s", database));
        return true;
    }

    public boolean tableExists(String database, String table) {
        return databaseExists(database) && listTables(database).contains(table);
    }

    public List<String> listTables(String databaseName) {
        if (!databaseExists(databaseName)) {
            throw new CreateTableException("database" + databaseName + " is not exists");
        }
        return extractColumnValuesBySQL(
                "SELECT TABLE_NAME FROM information_schema.`TABLES` WHERE TABLE_SCHEMA = ?",
                1,
                null,
                databaseName);
    }

    public void dropTable(String tableName) {
        execute(String.format("DROP TABLE IF EXISTS %s", tableName));
    }

    public void createTable(TableSchema schema) {
        String ddl = buildCreateTableDDL(schema);
        LOG.info("Create table with ddl:{}", ddl);
        execute(ddl);
    }

    public void execute(String sql) {
        try (Connection connection = jdbcConnectionProvider.getOrEstablishConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (Exception e) {
            LOG.error("SQL query could not be executed: {}", sql, e);
            throw new CreateTableException(
                    String.format("SQL query could not be executed: %s", sql), e);
        }
    }

    public List<String> extractColumnValuesBySQL(
            String sql, int columnIndex, Predicate<String> filterFunc, Object... params) {

        List<String> columnValues = Lists.newArrayList();
        try (Connection connection = jdbcConnectionProvider.getOrEstablishConnection();
                PreparedStatement ps = connection.prepareStatement(sql)) {
            if (Objects.nonNull(params) && params.length > 0) {
                for (int i = 0; i < params.length; i++) {
                    ps.setObject(i + 1, params[i]);
                }
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String columnValue = rs.getString(columnIndex);
                    if (filterFunc == null || filterFunc.test(columnValue)) {
                        columnValues.add(columnValue);
                    }
                }
            }
            return columnValues;
        } catch (Exception e) {
            throw new CreateTableException(
                    String.format("The following SQL query could not be executed: %s", sql), e);
        }
    }

    public static String buildCreateTableDDL(TableSchema schema) {
        return DorisSchemaFactory.generateCreateTableDDL(schema);
    }

    public Map<String, String> getTableFieldNames(String databaseName, String tableName) {
        if (!databaseExists(databaseName)) {
            throw new CreateTableException("database" + databaseName + " is not exists");
        }
        String sql =
                String.format(
                        "SELECT COLUMN_NAME,DATA_TYPE "
                                + "FROM `information_schema`.`COLUMNS` WHERE `TABLE_SCHEMA`= '%s' AND `TABLE_NAME`= '%s'",
                        databaseName, tableName);

        Map<String, String> columnValues = new HashMap<>();
        try (Connection connection = jdbcConnectionProvider.getOrEstablishConnection();
                PreparedStatement ps = connection.prepareStatement(sql);
                ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String fieldName = rs.getString(1);
                String datatype = rs.getString(2);
                columnValues.put(fieldName, datatype);
            }
            return columnValues;
        } catch (Exception e) {
            LOG.error("SQL query could not be executed: {}", sql, e);
            throw new CreateTableException(
                    String.format("The following SQL query could not be executed: %s", sql), e);
        }
    }

    @Deprecated
    public static String quoteDefaultValue(String defaultValue) {
        return DorisSchemaFactory.quoteDefaultValue(defaultValue);
    }

    @Deprecated
    public static String quoteComment(String comment) {
        return DorisSchemaFactory.quoteComment(comment);
    }

    @Deprecated
    public static String identifier(String name) {
        return DorisSchemaFactory.identifier(name);
    }

    @Deprecated
    public static String quoteTableIdentifier(String tableIdentifier) {
        return DorisSchemaFactory.quoteTableIdentifier(tableIdentifier);
    }
}
