package com.grid.queue.listener;

public interface NotificationListener {
    Channel channel();

    void onMessage(String payload);
}
