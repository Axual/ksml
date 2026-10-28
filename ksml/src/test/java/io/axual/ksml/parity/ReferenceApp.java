package io.axual.ksml.parity;

/*-
 * ========================LICENSE_START=================================
 * KSML
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

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;

/**
 * Runs the Kafka Streams reference topology of one parity case against a real broker. It is started in the
 * {@code reference} service of {@code parity-docker/docker-compose.yml}.
 * <p>
 * The case is found by calling its test method with {@code parity.mode=capture}: {@link ParityCase#run()}
 * then only stores the case, so the reference topology is the exact same code as in the test.
 */
public final class ReferenceApp {
    private ReferenceApp() {
    }

    public static void main(String[] args) throws Exception {
        final var testClass = System.getenv("REF_CLASS");
        final var testMethod = System.getenv("REF_METHOD");
        System.setProperty(ParityCase.MODE_PROPERTY, "capture");

        final var type = Class.forName(testClass);
        final var constructor = type.getDeclaredConstructor();
        constructor.setAccessible(true);
        final var method = type.getDeclaredMethod(testMethod);
        method.setAccessible(true);
        method.invoke(constructor.newInstance());
        final var parityCase = ParityCase.captured;

        final var props = DockerParity.streamsConfig(System.getenv("APP_ID"), System.getenv("BOOTSTRAP"), System.getenv("STATE_DIR"), parityCase.config);
        final var builder = new StreamsBuilder(new TopologyConfig(new StreamsConfig(props)));
        parityCase.reference.accept(builder);
        final var topology = builder.build(props);
        System.out.println("REFERENCE " + parityCase.id + " topology:\n" + topology.describe());

        final var stopped = new CountDownLatch(1);
        try (final var streams = new KafkaStreams(topology, props)) {
            streams.setUncaughtExceptionHandler(e -> {
                System.out.println("REFERENCE ERROR " + ParityRunner.describe(e));
                return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            });
            streams.setStateListener((newState, oldState) -> {
                System.out.println("REFERENCE STATE " + oldState + " -> " + newState);
                if (newState == KafkaStreams.State.ERROR || newState == KafkaStreams.State.NOT_RUNNING) stopped.countDown();
            });
            // Close inside the hook: the JVM exits when the hooks are done, so a close in main may never run
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                streams.close(Duration.ofSeconds(15));
                System.out.println("REFERENCE CLOSED " + streams.state());
                stopped.countDown();
            }));
            streams.start();
            stopped.await();
        }
    }
}
