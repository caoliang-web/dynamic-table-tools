package com.selectdb.dynamic.doris;

public class DorisType {
    public static final String BOOLEAN = "BOOLEAN";
    public static final String TINYINT = "TINYINT";
    public static final String SMALLINT = "SMALLINT";
    public static final String INT = "INT";
    public static final String BIGINT = "BIGINT";
    public static final String LARGEINT = "LARGEINT";
    // largeint is bigint unsigned in information_schema.COLUMNS
    public static final String BIGINT_UNSIGNED = "BIGINT UNSIGNED";
    public static final String FLOAT = "FLOAT";
    public static final String DOUBLE = "DOUBLE";
    public static final String DECIMAL = "DECIMAL";
    public static final String DECIMAL_V3 = "DECIMALV3";
    public static final String DATE = "DATE";
    public static final String DATE_V2 = "DATEV2";
    public static final String DATETIME = "DATETIME";
    public static final String DATETIME_V2 = "DATETIMEV2";
    public static final String CHAR = "CHAR";
    public static final String VARCHAR = "VARCHAR";
    public static final String STRING = "STRING";
    public static final String HLL = "HLL";
    public static final String BITMAP = "BITMAP";
    public static final String ARRAY = "ARRAY";
    public static final String JSONB = "JSONB";
    public static final String JSON = "JSON";
    public static final String MAP = "MAP";
    public static final String STRUCT = "STRUCT";
    public static final String VARIANT = "VARIANT";
    public static final String IPV4 = "IPV4";
    public static final String IPV6 = "IPV6";
}
