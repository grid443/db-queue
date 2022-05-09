package com.grid.queue.message;

import com.fasterxml.jackson.databind.JsonNode;
import com.grid.queue.DatabaseIntegrationTest;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.grid.queue.message.MessageState.CREATED;
import static com.grid.queue.message.MessageState.PROCESSED;
import static java.time.ZonedDateTime.now;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;

public class MessageRepositoryTest extends DatabaseIntegrationTest {

    @RepeatedTest(5)
    void should_process_messages_independently() throws Exception {
        // given
        var startLatch = new CountDownLatch(1);
        var tasksCount = 10;
        var finishLatch = new CountDownLatch(tasksCount);
        var workers = createWorkers(startLatch, finishLatch);

        // when
        workers.stream().map(Thread::new).forEach(Thread::start);
        startLatch.countDown();
        finishLatch.await();

        //then
        var processedMessageIds = workers.stream()
                .map(CoordinatedWorker::task)
                .map(StatefulTask::state)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(Message::id)
                .collect(toSet());
        assertThat(processedMessageIds).hasSize(tasksCount);
    }

    @Test
    void should_process_slow_tasks_independently() throws Exception {
        // given
        var messages = createMessages(10);
        assertThat(messages).hasSize(10);
        var finishLatch = new CountDownLatch(messages.size());
        var workers = createSlowTasks(messages, finishLatch);
        assertThat(workers).hasSize(10);

        // when
        workers.forEach(task -> runTaskWithDelay(task, MILLISECONDS, 300));
        finishLatch.await();

        // then
        var processedMessageIds = workers.stream()
                .map(ParallelWorker::task)
                .map(StatefulTask::state)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(Message::id)
                .collect(toSet());
        var messageIds = messages.stream().map(Message::id).collect(toSet());
        assertThat(messageIds)
                .hasSize(messages.size())
                .hasSize(processedMessageIds.size());
    }

    private Collection<ParallelWorker> createSlowTasks(Collection<Message> messages, CountDownLatch finishLatch) {
        return messages
                .stream()
                .map(__ -> new SlowStatefulTask())
                .map(task -> new ParallelWorker(task, finishLatch))
                .collect(Collectors.toList());
    }

    private Collection<Message> createMessages(int count) throws Exception {
        var messages = new ArrayList<Message>();
        var body = buildMessageBody();
        for (int i = 0; i < count; i++) {
            var message = new Message(randomUUID(), "test_queue", CREATED, body, now());
            repository.add(message);
            messages.add(message);
        }
        return messages;
    }

    private void runTaskWithDelay(ParallelWorker worker, TimeUnit delayUnit, int delay) {
        try {
            new Thread(worker).start();
            delayUnit.sleep(delay);
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    }

    private Collection<CoordinatedWorker> createWorkers(CountDownLatch startLatch,
                                                        CountDownLatch finishLatch) throws Exception {
        var workers = new ArrayList<CoordinatedWorker>();
        var body = buildMessageBody();
        long count = finishLatch.getCount();
        for (int i = 0; i < count; i++) {
            var message = new Message(randomUUID(), "test_queue", CREATED, body, now());
            repository.add(message);
            var task = new SimpleStatefulTask();
            var worker = new CoordinatedWorker(task, startLatch, finishLatch);
            workers.add(worker);
        }
        return workers;
    }

    private JsonNode buildMessageBody() throws Exception {
        var bodyString = """
                {"name": "value"}
                """;
        return mapper.readTree(bodyString);
    }

    private record ParallelWorker(StatefulTask task, CountDownLatch finishLatch) implements Runnable {

        @Override
        public void run() {
            processMessage(task);
            finishLatch.countDown();
        }
    }

    private record CoordinatedWorker(StatefulTask task,
                                     CountDownLatch startLatch,
                                     CountDownLatch finishLatch) implements Runnable {

        @Override
        public void run() {
            try {
                startLatch.await();
                processMessage(task);
                finishLatch.countDown();
            } catch (Exception e) {
                finishLatch.countDown();
                throw new AssertionError(e);
            }
        }
    }

    private static void processMessage(StatefulTask task) {
        var processedMessage = repository.processOldestTask(task);
        assertThat(processedMessage).isNotEmpty();
        var message = processedMessage.get();
        assertThat(message.state()).isEqualTo(PROCESSED);
    }
}
