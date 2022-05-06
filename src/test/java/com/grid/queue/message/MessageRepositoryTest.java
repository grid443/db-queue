package com.grid.queue.message;

import com.grid.queue.DatabaseIntegrationTest;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import org.junit.jupiter.api.RepeatedTest;

import static com.grid.queue.message.MessageState.CREATED;
import static com.grid.queue.message.MessageState.PROCESSED;
import static java.time.temporal.ChronoUnit.MICROS;
import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;

public class MessageRepositoryTest extends DatabaseIntegrationTest {

    @RepeatedTest(5)
    void should_process_messages_independently() throws Exception {
        // given
        var startLatch = new CountDownLatch(1);
        var tasksCount = 10;
        var finishLatch = new CountDownLatch(tasksCount);
        final var workers = createWorkers(startLatch, finishLatch);


        // when
        workers.stream().map(Thread::new).forEach(Thread::start);
        startLatch.countDown();
        finishLatch.await();

        //then
        var processedMessageIds = workers.stream()
                .map(Worker::task)
                .map(StatefulTask::state)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(Message::id)
                .collect(Collectors.toSet());
        assertThat(processedMessageIds).hasSize(tasksCount);
    }

    private Collection<Worker> createWorkers(CountDownLatch startLatch,
                                             CountDownLatch finishLatch) throws Exception {
        var workers = new ArrayList<Worker>();
        long count = finishLatch.getCount();
        var bodyString = """
                {"name": "value"}
                """;
        var body = mapper.readTree(bodyString);
        for (int i = 0; i < count; i++) {
            var message = new Message(randomUUID(), "test_queue", CREATED, body, ZonedDateTime.now().truncatedTo(MICROS));
            repository.add(message);
            var task = new SimpleStatefulTask();
            var worker = new Worker(task, startLatch, finishLatch);
            workers.add(worker);
        }
        return workers;
    }

    private record Worker(StatefulTask task,
                          CountDownLatch startLatch,
                          CountDownLatch finishLatch) implements Runnable {

        @Override
        public void run() {
            try {
                startLatch.await();
                var processedMessage = repository.processOldestTask(task);
                assertThat(processedMessage).isNotEmpty();
                var actualMessage = processedMessage.get();
                assertThat(actualMessage.state()).isEqualTo(PROCESSED);
                finishLatch.countDown();
            } catch (Exception e) {
                finishLatch.countDown();
                throw new AssertionError(e);
            }
        }
    }
}
