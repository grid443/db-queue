package com.grid.queue.listener;

public interface ListenerRegistration {
    void listen(NotificationListener listener);

    void shutdown();
}
