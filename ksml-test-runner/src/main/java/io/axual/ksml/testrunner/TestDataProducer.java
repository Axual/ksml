package io.axual.ksml.testrunner;

/*-
 * ========================LICENSE_START=================================
 * KSML Test Runner
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

import io.axual.ksml.generator.StreamDataType;
import io.axual.ksml.parser.UserTypeParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;

import java.time.Instant;
import java.util.List;

/**
 * Produces test data into TopologyTestDriver input topics from ProduceBlock definitions.
 * <p>
 * Uses KSML's {@link UserTypeParser} and {@link StreamDataType} to resolve type strings
 * (e.g. "avro:SensorData") into proper Kafka Serdes. This means raw YAML maps are
 * automatically converted through the full KSML serde pipeline:
 * NativeDataObjectMapper → notation-specific mapper (e.g. AvroDataObjectMapper) → vendor serializer.
 */
@Slf4j
public class TestDataProducer {

    private final TopologyTestDriver driver;

    public TestDataProducer(TopologyTestDriver driver) {
        this.driver = driver;
    }

    /**
     * Process all produce blocks, piping messages into the appropriate input topics.
     */
    public void produce(List<ProduceBlock> produceBlocks) {
        for (var block : produceBlocks) {
            produceBlock(block);
        }
    }

    private void produceBlock(ProduceBlock block) {
        log.debug("Producing {} messages to topic '{}'",
                block.messages() != null ? block.messages().size() : 0, block.topic());

        if (block.messages() != null) {
            produceInlineMessages(block);
        }
        // Generator-based production would be handled here in a future enhancement
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void produceInlineMessages(ProduceBlock block) {
        var keySerde = resolveSerde(block.keyType(), true);
        var valueSerde = resolveSerde(block.valueType(), false);

        TestInputTopic inputTopic = driver.createInputTopic(
                block.topic(), keySerde.serializer(), valueSerde.serializer());

        for (var msg : block.messages()) {
            if (msg.timestamp() != null) {
                inputTopic.pipeInput(msg.key(), msg.value(), Instant.ofEpochMilli(msg.timestamp()));
            } else {
                inputTopic.pipeInput(msg.key(), msg.value());
            }
        }
    }

    /**
     * Resolve a type string (e.g. "string", "avro:SensorData") to a Kafka Serde
     * using KSML's type resolution and notation serde pipeline.
     */
    private Serde<Object> resolveSerde(String type, boolean isKey) {
        if (type == null || type.equalsIgnoreCase("string")) {
            return new StringSerdeWrapper();
        }

        try {
            var parsed = new UserTypeParser().parse(type);
            if (parsed.isError()) {
                throw new TestDefinitionException("Cannot resolve type '" + type + "': " + parsed.errorMessage());
            }

            var userType = parsed.result();
            var streamDataType = new StreamDataType(userType, isKey);
            log.debug("Resolved type '{}' to StreamDataType: {}", type, streamDataType);
            return streamDataType.serde();
        } catch (TestDefinitionException e) {
            throw e;
        } catch (Exception e) {
            throw new TestDefinitionException("Exception resolving type '" + type + "': " + e.getMessage(), e);
        }
    }

    /**
     * Wrapper that adapts StringSerializer to a Serde&lt;Object&gt; so it can be used
     * uniformly with notation-based serdes.
     */
    private static class StringSerdeWrapper implements Serde<Object> {
        private final StringSerializer serializer = new StringSerializer();
        private final org.apache.kafka.common.serialization.StringDeserializer deserializer =
                new org.apache.kafka.common.serialization.StringDeserializer();

        @Override
        public org.apache.kafka.common.serialization.Serializer<Object> serializer() {
            return (topic, data) -> serializer.serialize(topic, data != null ? data.toString() : null);
        }

        @Override
        public org.apache.kafka.common.serialization.Deserializer<Object> deserializer() {
            return deserializer::deserialize;
        }
    }
}
