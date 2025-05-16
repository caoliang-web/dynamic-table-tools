package com.selectdb.dynamic.mysql;


import com.selectdb.dynamic.JdbcSourceSchema;
import java.sql.DatabaseMetaData;

public class MysqlSchema extends JdbcSourceSchema {

    public MysqlSchema(
            DatabaseMetaData metaData, String databaseName, String tableName, String tableComment)
            throws Exception {
        super(metaData, databaseName, null, tableName, tableComment);
    }

    public String convertToDorisType(String fieldType, Integer precision, Integer scale) {
        return MysqlType.toDorisType(fieldType, precision, scale);
    }

}
