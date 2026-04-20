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

import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.object.DataList;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataTuple;
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.generator.StreamDataType;
import io.axual.ksml.parser.UserTypeParser;
import io.axual.ksml.python.PythonContext;
import io.axual.ksml.python.PythonContextConfig;
import io.axual.ksml.python.PythonFunction;
import io.axual.ksml.type.UserTupleType;
import io.axual.ksml.type.UserType;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

    private static final NativeDataObjectMapper NATIVE_MAPPER = new NativeDataObjectMapper();
    private static final String GENERATOR_NAMESPACE = "test";

    private final TopologyTestDriver driver;
    private final Map<String, RegistryEntry> topicTypeMap;

    public TestDataProducer(TopologyTestDriver driver, Map<String, RegistryEntry> topicTypeMap) {
        this.driver = driver;
        this.topicTypeMap = topicTypeMap;
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
        if (block.messages() != null) {
            log.debug("Producing {} inline messages to topic '{}'", block.messages().size(), block.topic());
            produceInlineMessages(block);
        } else if (block.generator() != null) {
            long count = block.count() != null ? block.count() : 1;
            log.debug("Producing via generator ({} invocations) to topic '{}'", count, block.topic());
            produceGeneratedMessages(block, count);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void produceInlineMessages(ProduceBlock block) {
        var keySerde = resolveSerde(resolveKeyType(block), true);
        var valueSerde = resolveSerde(resolveValueType(block), false);

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

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void produceGeneratedMessages(ProduceBlock block, long count) {
        var generatorMap = block.generator();
        var generatorName = (String) generatorMap.getOrDefault("name", "test_generator");
        var globalCode = (String) generatorMap.getOrDefault("globalCode", "");
        var code = (String) generatorMap.getOrDefault("code", "");
        var expression = (String) generatorMap.getOrDefault("expression", "result");

        // Use a concrete ListType result type rather than GeneratorDefinition's UnionType default.
        // PythonDataObjectMapper cannot convert a polyglot Value when the expected type is a UnionType,
        // because polyglotValueToNative runs before the union dispatch. Real KSML generators always
        // specify an explicit resultType in YAML (e.g. "list(tuple(string, json))"), so this matches
        // established usage.
        var resultType = new UserType(new ListType(new UserTupleType(UserType.UNKNOWN, UserType.UNKNOWN)));
        var functionDef = FunctionDefinition.as(
                "generator", generatorName, List.of(),
                globalCode, code, expression, resultType, null);

        // Create a PythonContext and register the generator function
        var pythonContext = new PythonContext(PythonContextConfig.builder().build());
        var pythonFunction = PythonFunction.forGenerator(
                pythonContext, GENERATOR_NAMESPACE, generatorName, functionDef);

        // Set up the input topic with proper serdes
        var keySerde = resolveSerde(resolveKeyType(block), true);
        var valueSerde = resolveSerde(resolveValueType(block), false);
        TestInputTopic inputTopic = driver.createInputTopic(
                block.topic(), keySerde.serializer(), valueSerde.serializer());

        // Invoke the generator 'count' times and pipe results
        int totalMessages = 0;
        for (long i = 0; i < count; i++) {
            var generated = pythonFunction.call();
            var messages = extractKeyValuePairs(generated);
            for (var pair : messages) {
                inputTopic.pipeInput(pair[0], pair[1]);
                totalMessages++;
            }
        }
        log.debug("Generator produced {} messages to topic '{}'", totalMessages, block.topic());
    }

    /**
     * Extract key/value pairs from a generator result (always a list of tuples).
     */
    private List<Object[]> extractKeyValuePairs(DataObject generated) {
        var result = new ArrayList<Object[]>();
        if (generated instanceof DataList list) {
            for (DataObject element : list) {
                if (element instanceof DataTuple tuple && tuple.elements().size() == 2) {
                    result.add(new Object[]{
                            NATIVE_MAPPER.fromDataObject(tuple.elements().get(0)),
                            NATIVE_MAPPER.fromDataObject(tuple.elements().get(1))
                    });
                } else {
                    log.warn("Skipping invalid generator result element: {}", element);
                }
            }
        } else {
            log.warn("Generator returned unexpected result type: {}", generated != null ? generated.type() : "null");
        }
        return result;
    }

    /**
     * Resolve the effective key type for a produce block: use the block's own keyType if present,
     * otherwise fall back to the topic type map.
     */
    private String resolveKeyType(ProduceBlock block) {
        if (block.keyType() != null) return block.keyType();
        var entry = topicTypeMap.get(block.topic());
        return entry != null ? entry.keyType() : "string";
    }

    /**
     * Resolve the effective value type for a produce block: use the block's own valueType if present,
     * otherwise fall back to the topic type map.
     */
    private String resolveValueType(ProduceBlock block) {
        if (block.valueType() != null) return block.valueType();
        var entry = topicTypeMap.get(block.topic());
        return entry != null ? entry.valueType() : "string";
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
