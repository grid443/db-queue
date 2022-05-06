package com.grid.queue.message;

import java.util.Optional;

public interface MessageRepository {
    Optional<Message> processOldestTask(Task task);

    void add(Message message);
}
