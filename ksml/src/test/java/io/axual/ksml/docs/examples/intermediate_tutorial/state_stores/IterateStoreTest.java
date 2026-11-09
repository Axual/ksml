package io.axual.ksml.docs.examples.intermediate_tutorial.state_stores;

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

import io.axual.ksml.testutil.KSMLTest;
import io.axual.ksml.testutil.KSMLTestExtension;
import io.axual.ksml.testutil.KSMLTopic;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.extension.ExtendWith;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for the tutorial example that reads a store with a for loop: each output sums up the last readings of the
 * sensors with the same type and owner.
 */
@ExtendWith(KSMLTestExtension.class)
@SuppressWarnings("java:S2187")
public class IterateStoreTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @KSMLTopic(topic = "sensor_ownership_data")
    TestInputTopic<String, String> inputTopic;

    @KSMLTopic(topic = "owner_sensor_summary")
    TestOutputTopic<String, String> outputTopic;

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/state-stores/processor-iterate-store.yaml")
    void summarizesSensorsOfTheSameTypeAndOwner() {
        inputTopic.pipeInput("temperature_alice_01", reading(20.0));
        inputTopic.pipeInput("temperature_alice_02", reading(30.0));
        // Other owner and other type: not in the summary of temperature_alice
        inputTopic.pipeInput("temperature_bob_01", reading(90.0));
        inputTopic.pipeInput("humidity_alice_01", reading(70.0));
        // A new reading replaces the last one of the same sensor
        inputTopic.pipeInput("temperature_alice_01", reading(40.0));

        assertThat(outputTopic.readKeyValuesToList()).map(kv -> KeyValue.pair(kv.key, objectMapper.readTree(kv.value))).containsExactly(
                KeyValue.pair("temperature_alice_01", summary(1, 20.0)),
                KeyValue.pair("temperature_alice_02", summary(2, 25.0)),
                KeyValue.pair("temperature_bob_01", summary(1, 90.0)),
                KeyValue.pair("humidity_alice_01", summary(1, 70.0)),
                KeyValue.pair("temperature_alice_01", summary(2, 35.0)));
    }

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/state-stores/processor-iterate-store.yaml")
    void skipsRecordsWithoutASensorId() {
        inputTopic.pipeInput("no-sensor-id", reading(10.0));
        inputTopic.pipeInput("temperature_alice_01", (String) null);

        assertThat(outputTopic.isEmpty()).isTrue();
    }

    private String reading(double value) {
        return objectMapper.writeValueAsString(Map.of("owner", "any", "value", value));
    }

    private JsonNode summary(int sensors, double average) {
        return objectMapper.valueToTree(Map.of("sensors", sensors, "average", average));
    }
}
