package com.selectdb.dynamic.connection;

import java.sql.Connection;

public interface JdbcConnectionProvider {

    /** Get existing connection or establish an new one if there is none. */
    Connection getOrEstablishConnection() throws Exception;

    /** Close possible existing connection. */
    void closeConnection();
}
