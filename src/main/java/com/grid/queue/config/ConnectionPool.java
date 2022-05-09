package com.grid.queue.config;

import com.impossibl.postgres.jdbc.PGDataSource;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;

public class ConnectionPool {
    private final HikariDataSource ds;

    public ConnectionPool(DatabaseConnectionConfig connectionConfig) {
        final var config = new HikariConfig();
        config.setDataSource(createPgDataSource(connectionConfig));
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        ds = new HikariDataSource(config);
    }

    public DataSource dataSource() {
        return ds;
    }

    private DataSource createPgDataSource(DatabaseConnectionConfig config) {
        final var dataSource = new PGDataSource();
        dataSource.setServerName(config.host());
        dataSource.setPort(config.port());
        dataSource.setUser(config.username());
        dataSource.setPassword(config.password());
        dataSource.setDatabaseName(config.databaseName());
        return dataSource;
    }
}
