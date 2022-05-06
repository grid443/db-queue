package com.grid.queue.message;

import java.util.Optional;

public interface StatefulTask extends Task {
    Optional<Message> state();
}
