package com.grid.queue.validation;

public class Validation {
    public static <T> T required(String valueName, T value) {
        if (value == null) {
            throw new IllegalStateException(valueName + " is null");
        }
        return value;
    }
}
