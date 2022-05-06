package com.grid.queue.message;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.grid.queue.message.MessageState.CREATED;
import static java.lang.String.format;

public class SimpleStatefulTask implements StatefulTask {
    private static final Logger LOG = LoggerFactory.getLogger(LongStatefulTask.class);
    private static final String STATE_KEY = "STATE";

    private final Map<String, Message> state = new ConcurrentHashMap<>();

    @Override
    public void execute(Message message) {
        LOG.info("Execute Message[{}] State[{}]", message.id(), message.state());
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
