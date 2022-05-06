package com.grid.queue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.grid.queue.config.ConnectionPool;
import com.grid.queue.config.DatabaseConnectionConfig;
import com.grid.queue.config.DatabaseTestContainer;
import com.grid.queue.message.JdbcMessageRepository;
import com.grid.queue.message.MessageRepository;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.configuration.FluentConfiguration;

import static java.lang.Runtime.getRuntime;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class DatabaseIntegrationTest {
    private static final String PUBLIC_SCHEMA = "public";
    protected static final MessageRepository repository;
    protected static final ObjectMapper mapper;

    static {
        var config = new DatabaseConnectionConfig(5431, "test", "test", "message");
        var testContainer = new DatabaseTestContainer(config);
        testContainer.start();

        var connectionPool = new ConnectionPool(config);

        var migrationConfig = new FluentConfiguration()
                .dataSource(connectionPool.dataSource())
                .schemas(PUBLIC_SCHEMA);
        var flyway = new Flyway(migrationConfig);
        var result = flyway.migrate();
        assertThat(result.success).isTrue();
        assertThat(result.migrations).isNotEmpty();
        mapper = new ObjectMapper();
        repository = new JdbcMessageRepository(connectionPool.dataSource(), mapper);
        getRuntime().addShutdownHook(new Thread(flyway::clean));
        getRuntime().addShutdownHook(new Thread(testContainer::shutdown));
    }
}
