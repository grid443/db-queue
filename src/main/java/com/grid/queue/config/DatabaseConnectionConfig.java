package com.grid.queue.config;

import static com.grid.queue.validation.Validation.required;
import static java.lang.String.format;

public class DatabaseConnectionConfig {
    private static final String URL_TEMPLATE = "jdbc:postgresql://localhost:%s/%s";
    public final int port = 5432;
    public final String url;
    public final String username;
    public final String password;
    public final String databaseName;

    public DatabaseConnectionConfig(String username,
                                    String password,
                                    String databaseName) {
        this.username = required("username", username);
        this.password = required("password", password);
        this.databaseName = required("databaseName", databaseName);
        this.url = format(URL_TEMPLATE, port, databaseName);
    }
}
