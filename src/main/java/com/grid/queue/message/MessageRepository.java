package com.grid.queue.message;

import java.util.Optional;

public interface MessageRepository {
    Optional<Message> getLast();

    void add(Message message);
}
