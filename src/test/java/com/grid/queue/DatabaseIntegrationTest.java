package com.grid.queue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.grid.queue.config.ConnectionPool;
import com.grid.queue.config.DatabaseConnectionConfig;
import com.grid.queue.config.DatabaseTestContainer;
import com.grid.queue.message.JdbcMessageRepository;
import com.grid.queue.message.Message;
import java.time.ZonedDateTime;
import java.util.Optional;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.configuration.FluentConfiguration;
import org.junit.jupiter.api.Test;

import static com.grid.queue.message.MessageState.CREATED;
import static java.time.temporal.ChronoUnit.MICROS;
import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;

public class DatabaseIntegrationTest {

    private static final String PUBLIC_SCHEMA = "public";

    @Test
    void should_process_db_migrations() throws Exception {
        // given
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
        var mapper = new ObjectMapper();

        var repository = new JdbcMessageRepository(connectionPool.dataSource(), mapper);
        var bodyString = """
                {"name": "value"}
                """;
        var body = mapper.readTree(bodyString);
        var createdAt = ZonedDateTime.now().truncatedTo(MICROS);
        var message = new Message(randomUUID(), "test_queue", CREATED, body, createdAt);
        repository.add(message);

        // when
        var savedMessage = repository.getLast();

        //then
        assertThat(savedMessage).isEqualTo(Optional.of(message));

        // cleanup
        flyway.clean();
        testContainer.shutdown();
    }
}
