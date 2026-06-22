package io.axual.ksml.integration;

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

import io.axual.ksml.integration.testutil.ApicurioSchemaRegistryContainer;
import io.axual.ksml.integration.testutil.KSMLContainer;
import io.axual.ksml.integration.testutil.KSMLRunnerTestUtil;
import lombok.extern.slf4j.Slf4j;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue Axual/ksml#290.
 *
 * <p>Scenario: an AVRO record schema ({@code SensorData}) contains a NESTED named type
 * (an inline {@code enum SensorType}). The Apicurio AVRO SerDe is configured with
 * {@code apicurio.registry.auto-register: false}. The schema is PRE-REGISTERED in the
 * registry before KSML starts. The reporter observed that KSML serialization failed
 * because the schema lookup returned 404 / artifact not found (allegedly KSML reduced
 * the inline enum to a bare reference, producing a content mismatch).
 *
 * <p>This test pre-registers the {@code SensorData.avsc} (with the inline enum) under
 * artifactId {@code <topic>-value} in group {@code default}, then runs a KSML producer
 * that serializes {@code avro:SensorData} to that topic with auto-register disabled, and
 * checks whether messages actually land on the topic.
 *
 * <p><b>Status: passes, guarding the fix.</b> Without the fix this test FAILS: with the
 * issue-faithful config (only {@code auto-register: false} + {@code auto-register.if-exists:
 * RETURN}) Apicurio's content-based lookup represents the nested enum as a bare reference,
 * which never matches the inline-registered schema, so serialization fails with
 * {@code ArtifactNotFoundException}. The fix in {@code ApicurioAvroSerdeSupplier} defaults
 * {@code apicurio.registry.find-latest=true}, so the serializer resolves the artifact's
 * latest version by coordinates instead of by content. If this test starts failing again,
 * #290 has regressed.
 *
 * <p>Runs under maven-failsafe in the {@code integration-tests} profile (requires a running
 * Docker daemon for Testcontainers), or on its own via:
 * <pre>
 * mvn -pl ksml-integration-tests -am verify \
 *     -Dit.test=Issue290ApicurioNestedEnumNoAutoRegisterIT \
 *     -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false
 * </pre>
 */
@Slf4j
@Testcontainers
class Issue290ApicurioNestedEnumNoAutoRegisterIT {

    private static final String TOPIC = "sensor_data_avro_290";
    private static final String ARTIFACT_ID = TOPIC + "-value";
    private static final String GROUP = "default";

    static final Network network = Network.newNetwork();

    @AfterAll
    static void teardown() {
        network.close();
    }

    @Container
    static final KafkaContainer kafka = new KafkaContainer("apache/kafka:4.0.0")
            .withNetwork(network)
            .withNetworkAliases("broker")
            .withExposedPorts(9092, 9093);

    @Container
    static final ApicurioSchemaRegistryContainer schemaRegistry = new ApicurioSchemaRegistryContainer()
            .withNetwork(network)
            .withLegacyIdMode();

    @Container
    static final KSMLContainer ksml = new KSMLContainer()
            .withKsmlFiles("/issue290", "producer-avro.yaml", "SensorData.avsc")
            .withKafka(kafka)
            .withApicurioAvroRegistry(schemaRegistry)
            .withTopics(TOPIC)
            .withSetupCallback(Issue290ApicurioNestedEnumNoAutoRegisterIT::preRegisterSchema)
            .dependsOn(kafka, schemaRegistry);

    /**
     * Pre-registers the SensorData schema (WITH the nested inline enum) under
     * {@code default/<topic>-value} via the Apicurio v2 REST API, BEFORE KSML starts.
     */
    static void preRegisterSchema(KSMLContainer container) throws Exception {
        final int port = schemaRegistry.getMappedPort(8081);
        final String baseUrl = "http://localhost:" + port;

        KSMLRunnerTestUtil.waitForSchemaRegistryReady(baseUrl, Duration.ofSeconds(60));

        // Read the exact same avsc that KSML uses (the one with the inline SensorType enum).
        final var resource = Issue290ApicurioNestedEnumNoAutoRegisterIT.class
                .getResource("/issue290/SensorData.avsc");
        assertThat(resource).as("SensorData.avsc must be on the test classpath").isNotNull();
        final String avsc = Files.readString(Path.of(resource.toURI()));
        log.info("Issue290: pre-registering schema under {}/{}:\n{}", GROUP, ARTIFACT_ID, avsc);

        try (HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build()) {

            final HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl + "/apis/registry/v2/groups/" + GROUP + "/artifacts"))
                    .timeout(Duration.ofSeconds(30))
                    .header("Content-Type", "application/json")
                    .header("X-Registry-ArtifactId", ARTIFACT_ID)
                    .header("X-Registry-ArtifactType", "AVRO")
                    .POST(HttpRequest.BodyPublishers.ofString(avsc, StandardCharsets.UTF_8))
                    .build();

            final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            log.info("Issue290: pre-register response status={}, body={}", response.statusCode(), response.body());

            // 200/201 = registered, 409 = already exists. Anything else is a setup failure.
            assertThat(response.statusCode())
                    .as("Pre-registration of SensorData schema should succeed (got: %s / %s)",
                            response.statusCode(), response.body())
                    .isIn(200, 201, 409);

            // Confirm the artifact is retrievable, and log exactly what is stored.
            final HttpRequest get = HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl + "/apis/registry/v2/groups/" + GROUP
                            + "/artifacts/" + ARTIFACT_ID))
                    .timeout(Duration.ofSeconds(30))
                    .GET()
                    .build();
            final HttpResponse<String> getResp = client.send(get, HttpResponse.BodyHandlers.ofString());
            log.info("Issue290: stored artifact status={}, content=\n{}", getResp.statusCode(), getResp.body());
        }
    }

    @Test
    void reproducesIssue290() throws Exception {
        log.info("Issue290: waiting to see whether KSML can serialize avro:SensorData with auto-register=false...");

        boolean produced;
        try {
            // Producer emits every 3s. Give it generous time; if serialization fails,
            // no messages will ever appear and this throws.
            KSMLRunnerTestUtil.waitForTopicMessages(
                    kafka.getBootstrapServers(),
                    TOPIC,
                    2,
                    Duration.ofSeconds(45));
            produced = true;
        } catch (RuntimeException timeout) {
            produced = false;
            log.warn("Issue290: no messages produced within timeout -> serialization likely failed", timeout);
        }

        if (produced) {
            log.info("Issue290 VERDICT: DOES NOT REPRODUCE on this build. "
                    + "Messages were produced to '{}' with auto-register=false and a pre-registered "
                    + "nested-enum schema. The KSML/Apicurio serializer found and matched the schema.", TOPIC);
            assertThat(ksml.isRunning())
                    .as("KSML runner should still be running after successful serialization")
                    .isTrue();
        } else {
            log.error("Issue290 VERDICT: REPRODUCES. KSML failed to serialize avro:SensorData against the "
                    + "pre-registered nested-enum schema with auto-register=false. "
                    + "Inspect the KSML runner stdout above for the 404 / artifact-not-found / schema-reference error "
                    + "and the schema KSML actually sent.");
        }

        // The assertion encodes the expected outcome: we want to PROVE production works on this build.
        // If it fails, the failure (with logs above) is the reproduction evidence for #290.
        assertThat(produced)
                .as("Issue#290 reproduction: KSML should be able to serialize a nested-enum AVRO schema "
                        + "with apicurio auto-register=false when the schema is pre-registered. "
                        + "If this is false, #290 reproduces - see KSML runner logs for the 404 / reference error.")
                .isTrue();
    }
}
