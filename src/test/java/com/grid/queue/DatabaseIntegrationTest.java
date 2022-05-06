package com.grid.queue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.grid.queue.config.ConnectionPool;
import com.grid.queue.config.DatabaseConnectionConfig;
import com.grid.queue.config.DatabaseTestContainer;
import com.grid.queue.message.JdbcMessageRepository;
import com.grid.queue.message.LongStatefulTask;
import com.grid.queue.message.Message;
import com.grid.queue.message.MessageRepository;
import com.grid.queue.message.StatefulTask;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.configuration.FluentConfiguration;
import org.junit.jupiter.api.Test;

import static com.grid.queue.message.MessageState.CREATED;
import static com.grid.queue.message.MessageState.PROCESSED;
import static java.time.temporal.ChronoUnit.MICROS;
import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;

public class DatabaseIntegrationTest {

    private static final String PUBLIC_SCHEMA = "public";

    @Test
    void should_process_messages_independently() throws Exception {
        // given
        var config = new DatabaseConnectionConfig(5431, "test", "test", "message");
        var testContainer = new DatabaseTestContainer(config);
        testContainer.start();

        var connectionPool = new ConnectionPool(config);

        var migrationConfig = new FluentConfiguration()
                .dataSource(connectionPool.dataSource())
                .schemas(PUBLIC_SCHEMA);
        var flyway = new Flyway(migrationConfig);
        var result = flyway.migrate();
        assertThat(result.success).isTrue();
        assertThat(result.migrations).isNotEmpty();
        var mapper = new ObjectMapper();

        var repository = new JdbcMessageRepository(connectionPool.dataSource(), mapper);

        var startLatch = new CountDownLatch(1);
        var tasksCount = 10;
        var finishLatch = new CountDownLatch(tasksCount);
        final var workers = createWorkers(repository, mapper, startLatch, finishLatch);


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

        // cleanup
        flyway.clean();
        testContainer.shutdown();
    }

    private Collection<Worker> createWorkers(MessageRepository repository,
                                             ObjectMapper mapper,
                                             CountDownLatch startLatch,
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
            var task = new LongStatefulTask();
            var worker = new Worker(repository, task, startLatch, finishLatch);
            workers.add(worker);
        }
        return workers;
    }

    private record Worker(MessageRepository repository,
                          StatefulTask task,
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
