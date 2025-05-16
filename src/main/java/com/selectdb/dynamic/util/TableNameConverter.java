package com.selectdb.dynamic.util;

import java.io.Serializable;
import java.util.Map;
import java.util.regex.Pattern;

/*
 * Convert the table name of the upstream data source to the table name of the doris database.
 * */
public class TableNameConverter implements Serializable {

    private static final long serialVersionUID = 1L;
    private final String prefix;
    private final String suffix;

    // tbl_.*, tbl
    private Map<Pattern, String> routeRules;

    public TableNameConverter() {
        this("", "");
    }

    public TableNameConverter(String prefix, String suffix) {
        this.prefix = prefix == null ? "" : prefix;
        this.suffix = suffix == null ? "" : suffix;
    }

    public TableNameConverter(String prefix, String suffix, Map<Pattern, String> routeRules) {
        this.prefix = prefix == null ? "" : prefix;
        this.suffix = suffix == null ? "" : suffix;
        this.routeRules = routeRules;
    }

    public String convert(String tableName) {
        if (routeRules == null) {
            return prefix + tableName + suffix;
        }

        String target = null;

        for (Map.Entry<Pattern, String> patternStringEntry : routeRules.entrySet()) {
            if (patternStringEntry.getKey().matcher(tableName).matches()) {
                target = patternStringEntry.getValue();
            }
        }
        /**
         * If routeRules is not null and target is not assigned, then the synchronization task
         * contains both multi to one and one to one , prefixes and suffixes are added to common
         * one-to-one mapping tables
         */
        if (target == null) {
            return prefix + tableName + suffix;
        }
        return target;
    }
}
