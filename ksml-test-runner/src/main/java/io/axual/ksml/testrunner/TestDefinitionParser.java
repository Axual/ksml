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
import java.util.List;

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

        var fieldExtractor = new FieldExtractor(testNode, testFile);

        return new TestDefinition(
                fieldExtractor.requireString("name"),
                fieldExtractor.requireString("pipeline"),
                fieldExtractor.optionalString("schemaDirectory"),
                parseProduceBlocks(fieldExtractor.requireArray("produce"), testFile),
                parseAssertBlocks(fieldExtractor.requireArray("assert"), testFile)
        );
    }

    private List<ProduceBlock> parseProduceBlocks(JsonNode produceArray, Path testFile) {
        var blocks = new ArrayList<ProduceBlock>();
        for (var blockNode : produceArray) {
            var f = new FieldExtractor(blockNode, testFile);

            var topic = f.requireString("topic");
            var keyType = f.optionalString("keyType", "string");
            var valueType = f.optionalString("valueType", "string");
            var messages = parseMessages(blockNode.get("messages"));
            var generator = f.optionalMap("generator");
            var count = f.optionalLong("count");

            var block = new ProduceBlock(topic, keyType, valueType, messages, generator, count);
            block.validate();
            blocks.add(block);
        }
        return blocks;
    }

    private List<TestMessage> parseMessages(JsonNode messagesNode) {
        if (messagesNode == null || !messagesNode.isArray()) {
            return null;
        }
        var messages = new ArrayList<TestMessage>();
        for (var msgNode : messagesNode) {
            var key = FieldExtractor.nodeToObject(msgNode.get("key"));
            var value = FieldExtractor.nodeToObject(msgNode.get("value"));
            Long timestamp = null;
            if (msgNode.has("timestamp")) {
                timestamp = msgNode.get("timestamp").asLong();
            }
            messages.add(new TestMessage(key, value, timestamp));
        }
        return messages;
    }

    private List<AssertBlock> parseAssertBlocks(JsonNode assertArray, Path testFile) {
        var blocks = new ArrayList<AssertBlock>();
        for (var blockNode : assertArray) {
            var f = new FieldExtractor(blockNode, testFile);

            var topic = f.optionalString("topic");
            var code = f.requireString("code");
            var stores = f.optionalStringList("stores");

            var block = new AssertBlock(topic, stores, code);
            block.validate();
            blocks.add(block);
        }
        return blocks;
    }
}
