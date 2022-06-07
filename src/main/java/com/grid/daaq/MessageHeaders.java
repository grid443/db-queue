package com.grid.daaq;

public record MessageHeaders(Long timeToLive, String correlationID, Integer priority) {
}
