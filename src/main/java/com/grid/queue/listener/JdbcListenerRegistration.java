package com.grid.queue.listener;

import com.impossibl.postgres.api.jdbc.PGConnection;
import com.impossibl.postgres.api.jdbc.PGNotificationListener;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

public class JdbcListenerRegistration implements ListenerRegistration {
    private final Connection connection;

    public JdbcListenerRegistration(DataSource dataSource) {
        try {
            this.connection = dataSource.getConnection();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void listen(NotificationListener listener) {
        try {
            start(connection, listener.channel());
            final var pgListener = new PGNotificationListener() {
                @Override
                public void notification(int processId, String channelName, String payload) {
                    listener.onMessage(payload);
                }
            };
            final var pgConnection = connection.unwrap(PGConnection.class);
            pgConnection.addNotificationListener(listener.channel().name(), pgListener);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void shutdown() {
        try {
            connection.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void start(Connection connection, Channel channel) {
        try (final var statement = connection.createStatement();) {
            statement.execute("LISTEN " + channel.name());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
