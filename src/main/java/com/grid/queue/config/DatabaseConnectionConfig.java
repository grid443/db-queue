package com.grid.queue.config;

import static com.grid.queue.validation.Validation.required;

public record DatabaseConnectionConfig(String host,
                                       int port,
                                       String databaseName,
                                       String username,
                                       String password) {

    public DatabaseConnectionConfig(String host,
                                    int port,
                                    String databaseName,
                                    String username,
                                    String password) {
        this.host = required("host", host);
        this.port = port;
        this.databaseName = required("databaseName", databaseName);
        this.username = required("username", username);
        this.password = required("password", password);
    }
}
