package com.selectdb.dynamic.util;

import java.util.Map;
import java.util.Properties;

public class JdbcUrlUtils {

    public static final String PROPERTIES_PREFIX = "jdbc.properties.";

    public JdbcUrlUtils() {
    }

    public static Properties getJdbcProperties(Map<String, String> tableOptions) {
        Properties jdbcProperties = new Properties();
        if (hasJdbcProperties(tableOptions)) {
            tableOptions.keySet().stream().filter((key) -> {
                return key.startsWith("jdbc.properties.");
            }).forEach((key) -> {
                String value = (String)tableOptions.get(key);
                String subKey = key.substring("jdbc.properties.".length());
                jdbcProperties.put(subKey, value);
            });
        }

        return jdbcProperties;
    }

    private static boolean hasJdbcProperties(Map<String, String> tableOptions) {
        return tableOptions.keySet().stream().anyMatch((k) -> {
            return k.startsWith("jdbc.properties.");
        });
    }
}
