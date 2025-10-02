package io.axual.ksml.integration.testutil;

/*-
 * ========================LICENSE_START=================================
 * KSML Integration Tests
 * %%
 * Copyright (C) 2021 - 2025 Axual B.V.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * =========================LICENSE_END==================================
 */

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

/**
 * TestContainer for Apicurio Schema Registry.
 * Provides a pre-configured Apicurio Registry container with standard settings
 * for integration tests.
 */
public class ApicurioSchemaRegistryContainer extends GenericContainer<ApicurioSchemaRegistryContainer> {

    private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("apicurio/apicurio-registry:3.0.2");
    private static final int APICURIO_PORT = 8081;

    public ApicurioSchemaRegistryContainer() {
        this(DEFAULT_IMAGE_NAME);
    }

    public ApicurioSchemaRegistryContainer(DockerImageName dockerImageName) {
        super(dockerImageName);

        withExposedPorts(APICURIO_PORT);
        withEnv("QUARKUS_HTTP_PORT", String.valueOf(APICURIO_PORT));
        withEnv("QUARKUS_HTTP_CORS_ORIGINS", "*");
        withEnv("QUARKUS_PROFILE", "prod");
        withEnv("APICURIO_STORAGE_KIND", "sql");
        withEnv("APICURIO_STORAGE_SQL_KIND", "h2");
        waitingFor(Wait.forHttp("/apis").forPort(APICURIO_PORT).withStartupTimeout(Duration.ofMinutes(2)));
    }

    /**
     * Enables Confluent-compatible legacy ID mode.
     * This is required for compatibility with some Confluent Schema Registry clients.
     *
     * @return this container for method chaining
     */
    public ApicurioSchemaRegistryContainer withLegacyIdMode() {
        withEnv("APICURIO_CCOMPAT_LEGACY_ID_MODE_ENABLED", "true");
        return this;
    }

    /**
     * Configures this container to use a specific network with the alias "schema-registry".
     *
     * @param network The network to connect to
     * @return this container for method chaining
     */
    public ApicurioSchemaRegistryContainer withNetwork(Network network) {
        super.withNetwork(network);
        super.withNetworkAliases("schema-registry");
        return this;
    }
}
