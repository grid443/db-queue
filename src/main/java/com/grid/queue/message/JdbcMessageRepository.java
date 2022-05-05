package com.grid.queue.message;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Optional;
import java.util.TimeZone;
import java.util.UUID;
import javax.sql.DataSource;
import org.postgresql.util.PGobject;

import static com.grid.queue.validation.Validation.required;
import static java.util.Optional.empty;

public class JdbcMessageRepository implements MessageRepository {

    private static final String GET_LAST_MESSAGE_QUERY = """
            SELECT
              id,
              queue_name,
              state,
              body,
              created_at,
              to_char(created_at, '')
            FROM message
            WHERE state = 'CREATED'
            ORDER BY created_at DESC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
            """;

    private static final String INSERT_MESSAGE_QUERY = """
            INSERT INTO message
            (id, queue_name, state, body, created_at)
            VALUES
            (?, ?, ?, ?, ?);
            """;

    private final DataSource dataSource;
    private final ObjectMapper mapper;

    public JdbcMessageRepository(DataSource dataSource, ObjectMapper mapper) {
        this.dataSource = dataSource;
        this.mapper = mapper;
    }

    @Override
    public Optional<Message> getLast() {
        try (final var connection = dataSource.getConnection();
             final var statement = connection.prepareStatement(GET_LAST_MESSAGE_QUERY)) {
            final var resultSet = statement.executeQuery();
            if (resultSet.next()) {
                return Optional.of(fromResultSet(resultSet));
            } else {
                return empty();
            }
        } catch (Exception e) {
            throw new RuntimeException("Error getting the last message from the queue", e);
        }
    }

    @Override
    public void add(Message message) {
        try (final var connection = dataSource.getConnection();
             final var statement = connection.prepareStatement(INSERT_MESSAGE_QUERY)) {
            final var body = new PGobject();
            body.setType("JSONB");
            body.setValue(message.body().toString());
            final var createdAt = Timestamp.valueOf(message.createdAt().toLocalDateTime());
            statement.setObject(1, message.id());
            statement.setString(2, message.queueName());
            statement.setString(3, message.state().name());
            statement.setObject(4, body);
            statement.setObject(5, createdAt);
            statement.executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException("Error saving message " + message.id(), e);
        }
    }

    private Message fromResultSet(ResultSet resultSet) throws SQLException, JsonProcessingException {
        var id = UUID.fromString(required("message.id", resultSet.getString("id")));
        var queueName = required("message.queue_name", resultSet.getString("queue_name"));
        var state = MessageState.valueOf(required("message.state", resultSet.getString("state")));
        var bodyString = required("message.body", resultSet.getString("body"));
        var body = mapper.readTree(bodyString);
        var createdAtTimestamp = required("message.created_at", resultSet.getTimestamp("created_at", Calendar.getInstance(TimeZone.getTimeZone("UTC"))));
        var createdAt = ZonedDateTime.ofInstant(createdAtTimestamp.toInstant(), ZoneOffset.systemDefault());
        return new Message(id, queueName, state, body, createdAt);
    }
}
