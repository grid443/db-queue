package com.grid.queue.config;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.PortBinding;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import static com.github.dockerjava.api.model.Ports.Binding.bindPort;
import static com.grid.queue.validation.Validation.required;
import static java.lang.Runtime.getRuntime;
import static org.testcontainers.containers.PostgreSQLContainer.IMAGE;
import static org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT;

public class DatabaseTestContainer {
    private static final String PG_VERSION = "14";
    private final DatabaseConnectionConfig config;
    private final JdbcDatabaseContainer<?> container;

    public DatabaseTestContainer(DatabaseConnectionConfig config) {
        this.config = required("config", config);
        container = create();
        getRuntime().addShutdownHook(new Thread(this::shutdown));
    }

    public void start() {
        container.start();
    }

    public void shutdown() {
        container.stop();
    }

    private JdbcDatabaseContainer<?> create() {
        final var portBinding = new PortBinding(bindPort(POSTGRESQL_PORT), new ExposedPort(config.port));
        final var hostConfig = new HostConfig().withPortBindings(portBinding);
        return new PostgreSQLContainer<>(DockerImageName.parse(IMAGE).withTag(PG_VERSION))
                .withDatabaseName(config.databaseName)
                .withUsername(config.username)
                .withPassword(config.password)
                .withCreateContainerCmdModifier(cmd -> cmd.withHostConfig(hostConfig));
    }
}
