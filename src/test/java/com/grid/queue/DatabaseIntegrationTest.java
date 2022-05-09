package com.grid.queue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.grid.queue.config.ConnectionPool;
import com.grid.queue.config.DatabaseConnectionConfig;
import com.grid.queue.config.DatabaseTestContainer;
import com.grid.queue.listener.Channel;
import com.grid.queue.listener.JdbcListenerRegistration;
import com.grid.queue.listener.ListenerRegistration;
import com.grid.queue.listener.LoggingMessageNotificationListener;
import com.grid.queue.message.JdbcMessageRepository;
import com.grid.queue.message.MessageRepository;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.configuration.FluentConfiguration;

import static java.lang.Runtime.getRuntime;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class DatabaseIntegrationTest {
    private static final String PUBLIC_SCHEMA = "public";
    protected static final Channel CHANNEL = new Channel("msg");
    protected static final MessageRepository repository;
    protected static final ObjectMapper mapper;

    static {
        var config = new DatabaseConnectionConfig("localhost", 5431, "message", "test", "test");
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

        var listener = new LoggingMessageNotificationListener(CHANNEL);
        var listenerConfig = new JdbcListenerRegistration(connectionPool.dataSource());
        listenerConfig.listen(listener);
        getRuntime().addShutdownHook(new Thread(() -> shutdown(flyway, testContainer, listenerConfig)));
    }

    private static void shutdown(Flyway flyway, DatabaseTestContainer testContainer, ListenerRegistration messageListener) {
        flyway.clean();
        testContainer.shutdown();
        messageListener.shutdown();
    }
}
