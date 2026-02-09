package io.axual.ksml.example;

/*-
 * ========================LICENSE_START=================================
 * KSML Examples Test
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

import io.axual.ksml.operation.SensorData;
import io.axual.ksml.testutil.KSMLDriver;
import io.axual.ksml.testutil.KSMLTopic;
import io.axual.ksml.testutil.KSMLTopologyTest;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Validates that the example KSML topology definitions in the {@code examples/} directory
 * parse correctly and generate valid Kafka Streams topologies.
 * <p>
 * Excluded examples:
 * <ul>
 *   <li>{@code 01-example-inspect.yaml} — requires csv, jsonschema and protobuf notations</li>
 *   <li>{@code 07-example-convert.yaml} — requires xml notation</li>
 *   <li>{@code 09-example-aggregate2.yaml} — has a known parse error</li>
 *   <li>{@code 17-example-inspect-with-metrics.yaml} — requires csv and xml notations</li>
 * </ul>
 */
class ExampleTopologyTest {

    @KSMLTopic(topic = "ksml_sensordata_avro", valueSerde = KSMLTopic.SerdeType.AVRO)
    TestInputTopic<String, GenericRecord> sensorDataInput;

    @KSMLDriver
    TopologyTestDriver topologyTestDriver;

    @KSMLTopologyTest(topologies = {
            "examples/00-example-dump-messages.yaml",
            "examples/02-example-copy.yaml",
            "examples/03-example-filter.yaml",
            "examples/04-example-branch.yaml",
            "examples/05-example-route.yaml",
            "examples/06-example-duplicate.yaml",
            "examples/08-example-count.yaml",
            "examples/09-example-aggregate.yaml",
            "examples/10-example-queryable-table.yaml",
            "examples/11-example-field-modification.yaml",
            "examples/13-example-join.yaml",
            "examples/14-example-manual-state-store.yaml",
            "examples/15-example-pipeline-linking.yaml",
            "examples/16-example-transform-metadata.yaml",
            "examples/18-example-timestamp-extractor.yaml",
            "examples/19-example-performance-measurement.yaml"
    }, schemaDirectory = "examples")
    @DisplayName("Example topology parses and generates a valid Kafka Streams topology")
    void exampleTopologyIsValid() {
        assertNotNull(topologyTestDriver, "TopologyTestDriver should have been created by the test extension");

        // Pipe a single SensorData record to exercise the topology.
        sensorDataInput.pipeInput("sensor0", SensorData.builder()
                .name("sensor0")
                .timestamp(System.currentTimeMillis())
                .value("42")
                .type(SensorData.SensorType.TEMPERATURE)
                .unit("C")
                .color("blue")
                .city("Amsterdam")
                .owner("Alice")
                .build()
                .toRecord());
    }

    @KSMLTopologyTest(topologies = {
            "examples/12-example-byte-manipulation.yaml"
    }, schemaDirectory = "examples")
    @DisplayName("Byte manipulation example parses and generates a valid topology")
    void byteManipulationTopologyIsValid() {
        assertNotNull(topologyTestDriver, "TopologyTestDriver should have been created by the test extension");
    }

    @KSMLTopologyTest(topologies = {
        "examples/09-example-aggregate2.yaml"
    }, schemaDirectory = "examples")
    @DisplayName("09-example-aggregate2 parses and generates a valid topology")
    void exampleAggregate2ParsesAndGeneratesTopology() {
        assertNotNull(topologyTestDriver, "TopologyTestDriver should have been created by the test extension");
    }

}
