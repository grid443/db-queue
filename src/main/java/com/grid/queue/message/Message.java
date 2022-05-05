package com.grid.queue.message;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.ZonedDateTime;
import java.util.UUID;

import static com.grid.queue.validation.Validation.required;

public record Message(UUID id, String queueName, MessageState state, JsonNode body, ZonedDateTime createdAt) {

    public Message(UUID id, String queueName, MessageState state, JsonNode body, ZonedDateTime createdAt) {
        this.id = required("id", id);
        this.queueName = required("queueName", queueName);
        this.state = required("state", state);
        this.body = required("body", body);
        this.createdAt = required("createdAt", createdAt);
    }
}
