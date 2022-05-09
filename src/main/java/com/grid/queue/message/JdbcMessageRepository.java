package com.grid.queue.message;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.grid.queue.listener.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Optional;
import java.util.TimeZone;
import java.util.UUID;

import static com.grid.queue.message.MessageState.PROCESSED;
import static com.grid.queue.validation.Validation.required;
import static com.impossibl.postgres.api.jdbc.PGType.JSONB;
import static java.time.LocalDateTime.now;
import static java.util.Optional.empty;

public class JdbcMessageRepository implements MessageRepository {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcMessageRepository.class);

    private static final String GET_LAST_MESSAGE_QUERY = """
            SELECT
              id,
              queue_name,
              state,
              body,
              created_at
            FROM message
            WHERE state = 'CREATED'
            ORDER BY created_at ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
            """;

    private static final String UPDATE_MESSAGE_STATE_QUERY = """
            UPDATE message
            SET state = ?
            WHERE id = ?
            """;

    private static final String INSERT_MESSAGE_QUERY = """
            INSERT INTO message
            (id, queue_name, state, body, created_at)
            VALUES
            (?, ?, ?, ?, ?);
            """;

    private static final String NOTIFY_TEMPLATE = """
            NOTIFY %s , 'message.id[%s]'
            """;

    private final DataSource dataSource;
    private final ObjectMapper mapper;

    public JdbcMessageRepository(DataSource dataSource, ObjectMapper mapper) {
        this.dataSource = dataSource;
        this.mapper = mapper;
    }

    @Override
    public Optional<Message> processOldestTask(Task task) {
        try (final var connection = dataSource.getConnection();
             final var getMessage = connection.prepareStatement(GET_LAST_MESSAGE_QUERY);
             final var updateMessageState = connection.prepareStatement(UPDATE_MESSAGE_STATE_QUERY)) {
            try {
                connection.setAutoCommit(false);
                final var result = processMessage(getMessage, updateMessageState, task);
                connection.commit();
                return result;
            } catch (Exception e) {
                connection.rollback();
                throw e;
            }
        } catch (Exception e) {
            throw new RuntimeException("Error getting the last message from the queue", e);
        }
    }

    @Override
    public void add(Message message, Channel channel) {
        try (final var connection = dataSource.getConnection();
             final var statement = connection.prepareStatement(INSERT_MESSAGE_QUERY);
             final var notifyStatement = connection.createStatement()) {
            try {
                connection.setAutoCommit(false);
                final var createdAt = Timestamp.valueOf(message.createdAt().toLocalDateTime());
                final var body = message.body().toString();
                statement.setObject(1, message.id());
                statement.setString(2, message.queueName());
                statement.setString(3, message.state().name());
                statement.setObject(4, body, JSONB);
                statement.setObject(5, createdAt);
                statement.executeUpdate();
                notifyStatement.executeUpdate(String.format(NOTIFY_TEMPLATE, channel.name(), message.id()));
                connection.commit();
            } catch (Exception e) {
                connection.rollback();
            }
        } catch (Exception e) {
            throw new RuntimeException("Error saving message " + message.id(), e);
        }
    }

    private Optional<Message> processMessage(PreparedStatement getMessage,
                                             PreparedStatement updateMessageState,
                                             Task task) throws SQLException, JsonProcessingException {
        final var resultSet = getMessage.executeQuery();
        if (resultSet.next()) {
            return Optional.of(fromResultSet(resultSet))
                    .map(message -> executeTask(message, task, updateMessageState));
        } else {
            LOG.info("[{}] Cancel task. Message queue is empty", now());
            return empty();
        }
    }

    private Message executeTask(Message message, Task task, PreparedStatement updateMessageState) {
        try {
            LOG.info("[{}] START. Execute Message[{}] State[{}]", now(), message.id(), message.state());
            task.execute(message);
            updateMessageState.setString(1, PROCESSED.name());
            updateMessageState.setObject(2, message.id());
            int updated = updateMessageState.executeUpdate();
            if (updated != 1) {
                throw new IllegalStateException();
            }
            var processedMessage = message.updateState(PROCESSED);
            LOG.info("[{}] FINISH. Execute Message[{}] State[{}]", now(), processedMessage.id(), processedMessage.state());
            return processedMessage;
        } catch (Exception e) {
            throw new RuntimeException("Error processing task", e);
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
