package com.grid.queue.message;

public interface Task {
    void execute(Message message);
}
