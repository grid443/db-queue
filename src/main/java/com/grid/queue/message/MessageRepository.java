package com.grid.queue.message;

import com.grid.queue.listener.Channel;

import java.util.Optional;

public interface MessageRepository {
    Optional<Message> processOldestTask(Task task);

    void add(Message message, Channel channel);
}
