package com.grid.queue.message;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static com.grid.queue.message.MessageState.CREATED;
import static java.lang.String.format;

public class SimpleStatefulTask implements StatefulTask {
    private static final String STATE_KEY = "STATE";

    private final Map<String, Message> state = new ConcurrentHashMap<>();

    @Override
    public void execute(Message message) {
        if (!CREATED.equals(message.state())) {
            throw new IllegalStateException(format("Error processing Message[%s]. Illegal state [%s]", message.id(), message.state()));
        }
        state.put(STATE_KEY, message);
    }

    @Override
    public Optional<Message> state() {
        return Optional.of(state.get(STATE_KEY));
    }
}
