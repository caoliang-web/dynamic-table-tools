package com.selectdb.dynamic.connection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;

public class SimpleJdbcConnectionProvider implements JdbcConnectionProvider, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleJdbcConnectionProvider.class);
    private static final long serialVersionUID = 1L;

    private final DorisConnectionOptions options;

    private transient Connection connection;

    public SimpleJdbcConnectionProvider(DorisConnectionOptions options) {
        this.options = options;
    }

    @Override
    public Connection getOrEstablishConnection() throws ClassNotFoundException, SQLException {
        if (connection != null && !connection.isClosed() && connection.isValid(10)) {
            return connection;
        }
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException ex) {
            LOG.warn(
                    "can not found class com.mysql.cj.jdbc.Driver, use class com.mysql.jdbc.Driver");
            Class.forName("com.mysql.jdbc.Driver");
        }
        if (!Objects.isNull(options.getUsername())) {
            connection =
                    DriverManager.getConnection(
                            options.getJdbcUrl(), options.getUsername(), options.getPassword());
        } else {
            connection = DriverManager.getConnection(options.getJdbcUrl());
        }
        return connection;
    }

    @Override
    public void closeConnection() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                LOG.warn("JDBC connection close failed.", e);
            } finally {
                connection = null;
            }
        }
    }
}
