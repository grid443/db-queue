package com.grid.queue.model;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.UUID;

import static com.grid.queue.validation.Validation.required;

public class Queue {

    public final UUID id;
    public final String name;
    public final JsonNode message;

    public Queue(UUID id, String name, JsonNode message) {
        this.id = required("id", id);
        this.name = required("name", name);
        this.message = required("message", message);
    }
}
