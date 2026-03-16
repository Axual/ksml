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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Parses a YAML test definition file into a {@link TestDefinition}.
 */
@Slf4j
public class TestDefinitionParser {

    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

    public TestDefinition parse(Path testFile) throws IOException {
        log.debug("Parsing test definition from {}", testFile);
        var content = Files.readString(testFile);
        var root = YAML_MAPPER.readTree(content);

        var testNode = root.get("test");
        if (testNode == null) {
            throw new TestDefinitionException("Missing required 'test' root element in " + testFile);
        }

        var name = requireString(testNode, "name", testFile);
        var pipeline = requireString(testNode, "pipeline", testFile);
        var schemaDirectory = optionalString(testNode, "schemaDirectory");

        var produceBlocks = parseProduceBlocks(testNode, testFile);
        var assertBlocks = parseAssertBlocks(testNode, testFile);

        return new TestDefinition(name, pipeline, schemaDirectory, produceBlocks, assertBlocks);
    }

    private List<ProduceBlock> parseProduceBlocks(JsonNode testNode, Path testFile) {
        var produceNode = testNode.get("produce");
        if (produceNode == null || !produceNode.isArray()) {
            throw new TestDefinitionException("Missing or invalid 'produce' array in " + testFile);
        }

        var blocks = new ArrayList<ProduceBlock>();
        for (var blockNode : produceNode) {
            var topic = requireString(blockNode, "topic", testFile);
            var keyType = requireString(blockNode, "keyType", testFile);
            var valueType = requireString(blockNode, "valueType", testFile);

            List<TestMessage> messages = null;
            Map<String, Object> generator = null;
            Long count = null;

            var messagesNode = blockNode.get("messages");
            var generatorNode = blockNode.get("generator");

            if (messagesNode != null && messagesNode.isArray()) {
                messages = new ArrayList<>();
                for (var msgNode : messagesNode) {
                    var key = nodeToObject(msgNode.get("key"));
                    var value = nodeToObject(msgNode.get("value"));
                    Long timestamp = null;
                    if (msgNode.has("timestamp")) {
                        timestamp = msgNode.get("timestamp").asLong();
                    }
                    messages.add(new TestMessage(key, value, timestamp));
                }
            }

            if (generatorNode != null && generatorNode.isObject()) {
                generator = nodeToMap(generatorNode);
            }

            if (blockNode.has("count")) {
                count = blockNode.get("count").asLong();
            }

            if (messages == null && generator == null) {
                throw new TestDefinitionException(
                        "Produce block for topic '" + topic + "' must have either 'messages' or 'generator' in " + testFile);
            }

            blocks.add(new ProduceBlock(topic, keyType, valueType, messages, generator, count));
        }
        return blocks;
    }

    private List<AssertBlock> parseAssertBlocks(JsonNode testNode, Path testFile) {
        var assertNode = testNode.get("assert");
        if (assertNode == null || !assertNode.isArray()) {
            throw new TestDefinitionException("Missing or invalid 'assert' array in " + testFile);
        }

        var blocks = new ArrayList<AssertBlock>();
        for (var blockNode : assertNode) {
            var topic = optionalString(blockNode, "topic");
            var code = requireString(blockNode, "code", testFile);

            List<String> stores = null;
            var storesNode = blockNode.get("stores");
            if (storesNode != null && storesNode.isArray()) {
                stores = new ArrayList<>();
                for (var storeNode : storesNode) {
                    stores.add(storeNode.asText());
                }
            }

            if (topic == null && stores == null) {
                throw new TestDefinitionException(
                        "Assert block must have at least 'topic' or 'stores' in " + testFile);
            }

            blocks.add(new AssertBlock(topic, stores, code));
        }
        return blocks;
    }

    private String requireString(JsonNode parent, String field, Path testFile) {
        var node = parent.get(field);
        if (node == null || node.isNull()) {
            throw new TestDefinitionException("Missing required field '" + field + "' in " + testFile);
        }
        return node.asText();
    }

    private String optionalString(JsonNode parent, String field) {
        var node = parent.get(field);
        if (node == null || node.isNull()) {
            return null;
        }
        return node.asText();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> nodeToMap(JsonNode node) {
        return YAML_MAPPER.convertValue(node, LinkedHashMap.class);
    }

    private Object nodeToObject(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        if (node.isTextual()) {
            return node.asText();
        }
        if (node.isNumber()) {
            if (node.isInt()) return node.asInt();
            if (node.isLong()) return node.asLong();
            return node.asDouble();
        }
        if (node.isBoolean()) {
            return node.asBoolean();
        }
        if (node.isObject()) {
            return nodeToMap(node);
        }
        if (node.isArray()) {
            var list = new ArrayList<>();
            for (var item : node) {
                list.add(nodeToObject(item));
            }
            return list;
        }
        return node.asText();
    }
}
