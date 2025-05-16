package com.selectdb.dynamic.oracle;

import com.selectdb.dynamic.DatabaseSync;
import com.selectdb.dynamic.SourceSchema;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class OracleDatabaseSync extends DatabaseSync {

    public OracleDatabaseSync() throws SQLException {
        super();
    }
    @Override
    public void registerDriver() throws SQLException {

    }

    @Override
    public Connection getConnection() throws SQLException {
        return null;
    }

    @Override
    public List<SourceSchema> getSchemaList() throws Exception {
        return null;
    }

    @Override
    public String getTableListPrefix() {
        return null;
    }
}
