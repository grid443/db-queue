package com.grid.queue.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.postgresql.ds.PGSimpleDataSource;

public class ConnectionPool {
    private final HikariDataSource ds;

    public ConnectionPool(DatabaseConnectionConfig connectionConfig) {
        final var dataSource = new PGSimpleDataSource();
        dataSource.setUrl(connectionConfig.url);
        dataSource.setUser(connectionConfig.username);
        dataSource.setPassword(connectionConfig.password);
        dataSource.setDatabaseName(connectionConfig.databaseName);
        final var config = new HikariConfig();
        config.setDataSource(dataSource);
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        ds = new HikariDataSource(config);
    }

    public DataSource dataSource() {
        return ds;
    }

    public Connection getConnection() throws SQLException {
        return ds.getConnection();
    }
}
