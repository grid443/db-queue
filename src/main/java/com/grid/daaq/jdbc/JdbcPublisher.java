package com.grid.daaq.jdbc;

import com.grid.daaq.MessageHeaders;
import com.grid.daaq.Publisher;
import com.grid.daaq.Receipt;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.function.Function;

public class JdbcPublisher<PayloadType> implements Publisher<PayloadType, Connection> {

    private final Function<PayloadType, Object[]> serializer;
    private final String insertSQL;

    public JdbcPublisher(JdbcTopicConfig config, Function<PayloadType, Object[]> toArray) {
        this.serializer = toArray;
        this.insertSQL = "insert into " + config.tableName() + " (..." + String.join(",", config.columnNames())
                + " (?, ?, ...";
    }

    @Override
    public Receipt publish(Connection connection, MessageHeaders headers, PayloadType payload) {
        Object[] values = serializer.apply(payload);
        try {
            PreparedStatement statement = connection.prepareStatement(insertSQL);
            statement.setObject(0, headers.correlationID());
            statement.setObject(1, headers.timeToLive());
            // set headers, then values
            statement.setObject(3, values[0]);
            statement.executeUpdate(insertSQL);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return new Receipt("111", null);
    }

    private static void useIt(Connection connection) {
        JdbcTopicConfig config = new JdbcTopicConfig("queue1", new String[] {"col1", "col2"});
        Function<Object[], Object[]> serializer = a -> a;
        var publisher = new JdbcPublisher<>(config, serializer);

        var headers = new MessageHeaders(10L, "123", 1);

        Object[] payload = new String[] {"a"};
        Receipt receipt = publisher.publish(connection, headers, payload);
    }
}
