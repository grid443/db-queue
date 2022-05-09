package com.grid.queue.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingMessageNotificationListener implements NotificationListener {

    private static final Logger LOG = LoggerFactory.getLogger(LoggingMessageNotificationListener.class);

    private final Channel channel;

    public LoggingMessageNotificationListener(Channel channel) {
        this.channel = channel;
    }

    @Override
    public Channel channel() {
        return channel;
    }

    public void onMessage(String payload) {
        LOG.info("RECEIVED MESSAGE from [{}] Payload [{}]", channel.name(), payload);
    }
}
