package io.axual.ksml.integration.testutil;

/*-
 * ========================LICENSE_START=================================
 * KSML Integration Tests
 * %%
 * Copyright (C) 2021 - 2026 Axual B.V.
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

import org.testcontainers.containers.Network;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Shared Testcontainers infrastructure for the integration tests: a single Kafka broker and a single
 * Apicurio schema registry, started once and reused by every IT class, instead of each IT starting its
 * own stack. Starting these containers is the dominant cost of the IT suite, so sharing them removes
 * most of that repeated startup time.
 *
 * <p>Each container is started lazily on first use (so IT classes that need no registry never start one)
 * and never explicitly stopped; Testcontainers' Ryuk reaps them when the JVM exits. In practice "first
 * use" is class-load time, because IT classes call {@link #kafka()}/{@link #schemaRegistry()} from their
 * static {@code @Container} field initializers, not from within a test method. Because they are
 * shared, ITs MUST use unique Kafka topic names so they do not collide on the broker or on
 * schema-registry subjects.
 *
 * <p>The registry serves both the Apicurio-native ({@code /apis/registry/v2}) and Confluent-compatible
 * ({@code /apis/ccompat/v7}) endpoints, covering every registry-based IT. It runs with legacy-id mode
 * ({@code APICURIO_CCOMPAT_LEGACY_ID_MODE_ENABLED=true}), which changes the schema id the ccompat endpoint
 * embeds in the wire-format header read by the Confluent/JSON serdes. Do not remove it: before these
 * containers were consolidated {@code ApicurioAvroSchemaRegistryIT} already enabled legacy-id mode while
 * {@code ConfluentAvroSchemaRegistryIT} and {@code JsonSchemaRegistryIT} did not, so turning it on for the
 * shared registry is a deliberate, verified environment change for those two ccompat-based tests, not an
 * accident to clean up.
 *
 * <p>The instances are per-JVM. Under parallel test execution each Surefire/Failsafe fork gets its own
 * Kafka and registry, so forks stay isolated; within a JVM, tests must run sequentially (which is the
 * default) as the containers are shared mutable state.
 */
public final class SharedKsmlInfra {

    private static final Network NETWORK = Network.newNetwork();

    private static KafkaContainer kafka;
    private static ApicurioSchemaRegistryContainer schemaRegistry;

    private SharedKsmlInfra() {
    }

    /** The shared Kafka broker, started on first access. */
    public static synchronized KafkaContainer kafka() {
        if (kafka == null) {
            kafka = new KafkaContainer("apache/kafka:4.0.0")
                    .withNetwork(NETWORK)
                    .withNetworkAliases("broker")
                    .withExposedPorts(9092, 9093);
            kafka.start();
        }
        return kafka;
    }

    /** The shared Apicurio schema registry (legacy-id mode), started on first access. */
    public static synchronized ApicurioSchemaRegistryContainer schemaRegistry() {
        if (schemaRegistry == null) {
            schemaRegistry = new ApicurioSchemaRegistryContainer()
                    .withNetwork(NETWORK)
                    .withLegacyIdMode();
            schemaRegistry.start();
        }
        return schemaRegistry;
    }
}
