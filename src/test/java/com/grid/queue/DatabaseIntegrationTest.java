package com.grid.queue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.grid.queue.config.ConnectionPool;
import com.grid.queue.config.DatabaseConnectionConfig;
import com.grid.queue.config.DatabaseTestContainer;
import com.grid.queue.model.Queue;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.configuration.FluentConfiguration;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class DatabaseIntegrationTest {

    private static final String PUBLIC_SCHEMA = "public";

    @Test
    void should_process_db_migrations() {
        // given
        var config = new DatabaseConnectionConfig("test", "test", "queue");
        var testContainer = new DatabaseTestContainer(config);
        testContainer.start();

        var connectionPool = new ConnectionPool(config);

        var migrationConfig = new FluentConfiguration()
                .dataSource(connectionPool.dataSource())
                .schemas(PUBLIC_SCHEMA);
        var flyway = new Flyway(migrationConfig);
        var result = flyway.migrate();
        assertThat(result.success).isTrue();
        assertThat(result.migrations).hasSize(1);
        var mapper = new ObjectMapper(); // TODO move to the configuration

        var query = """
                        SELECT id,
                               name,
                               message
                        FROM queue
                """;

        try (var connection = connectionPool.getConnection(); var statement = connection.prepareStatement(query)) {

            // when
            var resultSet = statement.executeQuery();

            // then
            var queueList = new ArrayList<Queue>();
            while (resultSet.next()) {
                var id = UUID.fromString(resultSet.getString("id"));
                var name = resultSet.getString("name");
                var messageString = resultSet.getString("message");
                var message = mapper.readTree(messageString);
                queueList.add(new Queue(id, name, message));
            }
            assertThat(queueList).hasSize(1);
            var queue = queueList.get(0);
            assertThat(queue.id).isEqualTo(UUID.fromString("466b4029-6eff-4698-b854-01bf9cdfd091"));
            assertThat(queue.name).isEqualTo("test");
            var expectedMessage = """
                    {"name": "value"}
                    """;
            assertThat(queue.message).isEqualTo(mapper.readTree(expectedMessage));
        } catch (SQLException e) {
            fail("SQL failure", e);
        } catch (JsonProcessingException e) {
            fail("JSON parsing failure", e);
        }

        flyway.clean();
        testContainer.shutdown();
    }
}
