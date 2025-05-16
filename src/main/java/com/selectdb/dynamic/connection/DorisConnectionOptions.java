package com.selectdb.dynamic.connection;


import com.selectdb.dynamic.util.Preconditions;

import java.io.Serializable;

/** Doris connection options. */
public class DorisConnectionOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String username;
    private final String password;
    private String jdbcUrl;

    public DorisConnectionOptions(
            String username, String password, String jdbcUrl) {
        this.username = username;
        this.password = password;
        this.jdbcUrl = jdbcUrl;
    }




    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }


    public String getJdbcUrl() {
        return jdbcUrl;
    }


    /** Builder for {@link DorisConnectionOptions}. */
    public static class DorisConnectionOptionsBuilder {
        private String username;
        private String password;
        private String jdbcUrl;

        public DorisConnectionOptionsBuilder withUsername(String username) {
            this.username = username;
            return this;
        }

        public DorisConnectionOptionsBuilder withPassword(String password) {
            this.password = password;
            return this;
        }

        public DorisConnectionOptionsBuilder withJdbcUrl(String jdbcUrl) {
            this.jdbcUrl = jdbcUrl;
            return this;
        }

        public DorisConnectionOptions build() {
            return new DorisConnectionOptions(
                     username, password, jdbcUrl);
        }
    }
}
