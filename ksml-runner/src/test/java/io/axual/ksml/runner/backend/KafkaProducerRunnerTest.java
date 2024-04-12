package io.axual.ksml.runner.backend;

/*-
 * ========================LICENSE_START=================================
 * KSML Runner
 * %%
 * Copyright (C) 2021 - 2024 Axual B.V.
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

import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.definition.ProducerDefinition;
import io.axual.ksml.definition.TopicDefinition;
import io.axual.ksml.generator.TopologyDefinition;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.graalvm.home.Version;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
@EnabledIf(value = "isRunningOnGraalVM", disabledReason = "This test needs GraalVM to work")
public class KafkaProducerRunnerTest {

    @Mock
    private KafkaProducer<byte[], byte[]> mockProducer;

    private KafkaProducerRunner.Config testConfig;

    private KafkaProducerRunner producerRunner;

    @BeforeEach
    void setup() {
        testConfig = new KafkaProducerRunner.Config(new HashMap<>(), new HashMap<>());
        producerRunner = new KafkaProducerRunner(testConfig) {
            @Override
            protected KafkaProducer<byte[], byte[]> createProducer(Map<String, Object> config) {
                return mockProducer;
            }
        };
    }

    @Test
    void minimalSmoketest() throws InterruptedException {
        TopologyDefinition topologyDefinition = new TopologyDefinition("test");
        FunctionDefinition mockFunction = mock(FunctionDefinition.class);
        TopicDefinition mockTopic = mock(TopicDefinition.class);
        ProducerDefinition producerDefinition = new ProducerDefinition(mockFunction, null, null, mockTopic, null, null);
        topologyDefinition.register("testProducer", producerDefinition);
        testConfig.definitions().put("testTopology", topologyDefinition);

        // schedule a stop for the runner loop
        try (ScheduledExecutorService stopper = Executors.newSingleThreadScheduledExecutor()) {
            stopper.schedule(() -> producerRunner.stop(), 2, TimeUnit.SECONDS);
            producerRunner.run();
        }
    }

    static boolean isRunningOnGraalVM() {
        return Version.getCurrent().isRelease();
    }

}
