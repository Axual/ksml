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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Parses a YAML test suite definition file into a {@link TestSuiteDefinition}.
 *
 * <p>Format overview: a flat YAML document (no outer wrapper) with top-level fields
 * {@code name} (optional), {@code definition} (required, path to a KSML pipeline definition
 * YAML), {@code schemaDirectory}, {@code moduleDirectory}, {@code streams} (map keyed by
 * logical stream name with topic/keyType/valueType), and {@code tests} (map keyed by
 * stable identifier with per-test {@code description}, {@code produce}, {@code assert}).
 */
@Slf4j
public class TestDefinitionParser {

    /** Regex enforced for both stream-map keys and test-map keys. */
    public static final Pattern IDENTIFIER_PATTERN = Pattern.compile("^[a-zA-Z][a-zA-Z0-9_]*$");

    /** Top-level fields permitted at the suite level. Anything else triggers a "did you misplace this?" error when it appears under a test entry. */
    private static final Set<String> SUITE_LEVEL_FIELDS = Set.of(
            "name", "definition", "schemaDirectory", "moduleDirectory", "streams", "tests");

    // STRICT_DUPLICATE_DETECTION makes Jackson throw on duplicate keys at any nesting level
    // instead of silently keeping one of them. The thrown JsonParseException carries line/column
    // info we surface in the error message.
    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory())
            .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);

    /**
     * Parse a test suite definition file into a {@link TestSuiteDefinition}.
     * @param testFile {@link @link Path} to the test suite definition file.
     * @return a TestSuiteDefinition object.
     * @throws IOException if the file cannot be read or parsed.
     * @throws TestDefinitionException if the file is malformed.
     */
    public TestSuiteDefinition parse(Path testFile) throws IOException {
        log.debug("Parsing test suite definition from {}", testFile);
        var content = Files.readString(testFile);

        JsonNode root;
        try {
            root = YAML_MAPPER.readTree(content);
        } catch (JsonProcessingException e) {
            // Catches duplicate-key detection and other YAML-level malformation
            throw new TestDefinitionException(
                    "Invalid YAML in " + testFile + ": " + e.getOriginalMessage(), e);
        }
        if (root == null || !root.isObject()) {
            throw new TestDefinitionException("Top-level YAML node must be an object in " + testFile);
        }

        var fieldExtractor = new FieldExtractor(root, testFile);

        var declaredName = fieldExtractor.optionalString("name");
        var name = (declaredName != null && !declaredName.isEmpty())
                ? declaredName
                : filenameWithoutExtension(testFile);
        var definition = fieldExtractor.requireString("definition");
        var schemaDirectory = fieldExtractor.optionalString("schemaDirectory");
        var moduleDirectory = fieldExtractor.optionalString("moduleDirectory");

        var streams = parseStreams(root.get("streams"), testFile);
        var tests = parseTests(root.get("tests"), streams.keySet(), testFile);

        return new TestSuiteDefinition(name, definition, schemaDirectory, moduleDirectory, streams, tests);
    }

    /**
     * Return the filename portion of {@code path} without its extension.
     */
    public static String filenameWithoutExtension(Path path) {
        var name = path.getFileName().toString();
        var dot = name.lastIndexOf('.');
        return dot > 0 ? name.substring(0, dot) : name;
    }

    /**
     * Build a topic-to-stream map for callers that need to look up types by topic name
     * (e.g., the assertion runner's deserializer resolution and the schema registry population).
     * @param suite the parsed test suite definition.
     * @return a map of topic name to stream definition.
     */
    public static Map<String, StreamDefinition> buildTopicTypeMap(TestSuiteDefinition suite) {
        var map = new LinkedHashMap<String, StreamDefinition>();
        if (suite.streams() != null) {
            for (var entry : suite.streams().values()) {
                map.put(entry.topic(), entry);
            }
        }
        return map;
    }

    /**
     * Parse the streams map from the YAML test definition.
     * @param streamsNode the streams node in the YAML test definition.
     * @param testFile the path to the test suite definition file.
     * @return a map of stream key to stream definition.
     */
    private LinkedHashMap<String, StreamDefinition> parseStreams(JsonNode streamsNode, Path testFile) {
        var result = new LinkedHashMap<String, StreamDefinition>();
        if (streamsNode == null || streamsNode.isNull()) {
            throw new TestDefinitionException("Missing required 'streams' map in " + testFile);
        }
        if (!streamsNode.isObject()) {
            throw new TestDefinitionException("'streams' must be a map in " + testFile);
        }
        if (streamsNode.isEmpty()) {
            throw new TestDefinitionException(
                    "'streams' map must contain at least one entry in " + testFile);
        }

        var seenTopics = new HashMap<String, String>(); // topic -> first stream key that used it
        var fields = streamsNode.properties();
        for (var entry : fields) {
            var streamKey = entry.getKey();
            validateIdentifier("stream", streamKey, testFile);

            var entryNode = entry.getValue();
            if (!entryNode.isObject()) {
                throw new TestDefinitionException(
                        "Stream entry '" + streamKey + "' must be an object in " + testFile);
            }
            var ef = new FieldExtractor(entryNode, testFile);
            var topic = ef.requireString("topic");
            var keyType = ef.optionalString("keyType", "string");
            var valueType = ef.optionalString("valueType", "string");

            // Note: type-string validation (e.g., rejection of bare 'confluent_avro' or 'json:Foo')
            // is deferred to runtime. UserTypeParser depends on the global notation library, which
            // is registered in TestExecutionContext.setup() which happens after parsing. Errors
            // surface as a runtime ERROR result when the runner tries to construct a serde for
            // the type.

            // Reject duplicate topic across streams
            var existing = seenTopics.put(topic, streamKey);
            if (existing != null) {
                throw new TestDefinitionException(
                        "Streams '" + existing + "' and '" + streamKey + "' both reference topic '"
                                + topic + "'; each topic may appear at most once in 'streams' (" + testFile + ")");
            }

            result.put(streamKey, new StreamDefinition(topic, keyType, valueType));
        }
        return result;
    }

    /**
     * Parse the tests map from the YAML test definition.
     * @param testsNode the tests node in the YAML test definition.
     * @param streamKeys the keys of the streams map.
     * @param testFile the path to the test suite definition file.
     * @return a map of test key to test definition.
     */
    private LinkedHashMap<String, TestCaseDefinition> parseTests(JsonNode testsNode,
                                                                 Set<String> streamKeys,
                                                                 Path testFile) {
        if (testsNode == null || testsNode.isNull()) {
            throw new TestDefinitionException("Missing required 'tests' map in " + testFile);
        }
        if (!testsNode.isObject()) {
            throw new TestDefinitionException("'tests' must be a map in " + testFile);
        }
        if (testsNode.isEmpty()) {
            throw new TestDefinitionException(
                    "'tests' map must contain at least one entry in " + testFile);
        }

        var result = new LinkedHashMap<String, TestCaseDefinition>();
        var fields = testsNode.properties();
        for (var entry : fields) {
            var testKey = entry.getKey();
            validateIdentifier("test", testKey, testFile);

            var entryNode = entry.getValue();
            if (!entryNode.isObject()) {
                throw new TestDefinitionException(
                        "Test entry '" + testKey + "' must be an object in " + testFile);
            }

            // Reject suite-level fields appearing inside a test entry
            entryNode.fieldNames().forEachRemaining(field -> {
                if (SUITE_LEVEL_FIELDS.contains(field)) {
                    throw new TestDefinitionException(
                            "Field '" + field + "' is a suite-level field and cannot appear under test '"
                                    + testKey + "' (" + testFile + ")");
                }
            });

            var description = optionalText(entryNode.get("description"));
            var produceNode = entryNode.get("produce");
            if (produceNode == null) {
                throw new TestDefinitionException(
                        "Test '" + testKey + "' is missing required field 'produce' in " + testFile);
            }
            if (!produceNode.isArray()) {
                throw new TestDefinitionException(
                        "Test '" + testKey + "' field 'produce' must be an array in " + testFile);
            }
            var assertNode = entryNode.get("assert");
            if (assertNode == null) {
                throw new TestDefinitionException(
                        "Test '" + testKey + "' is missing required field 'assert' in " + testFile);
            }
            if (!assertNode.isArray()) {
                throw new TestDefinitionException(
                        "Test '" + testKey + "' field 'assert' must be an array in " + testFile);
            }

            var produceBlocks = parseProduceBlocks(produceNode, streamKeys, testKey, testFile);
            var assertBlocks = parseAssertBlocks(assertNode, streamKeys, testKey, testFile);

            result.put(testKey, new TestCaseDefinition(description, produceBlocks, assertBlocks));
        }
        return result;
    }

    /**
     * Parse the produce blocks from the YAML test definition.
     * @param produceArray the produce array in the YAML test definition.
     * @param streamKeys the keys of the streams map.
     * @param testKey the name of the test in the YAML test definition.
     * @param testFile the path to the test suite definition file.
     * @return a list of {@link ProduceBlock}.
     */
    private List<ProduceBlock> parseProduceBlocks(JsonNode produceArray, Set<String> streamKeys,
                                                  String testKey, Path testFile) {
        var blocks = new ArrayList<ProduceBlock>();
        for (var blockNode : produceArray) {
            var f = new FieldExtractor(blockNode, testFile);
            var to = f.requireString("to");
            if (!streamKeys.contains(to)) {
                throw new TestDefinitionException(
                        "Produce block in test '" + testKey + "' references stream '" + to
                                + "' that is not declared in 'streams:' (" + testFile + ")");
            }
            var messages = parseMessages(blockNode.get("messages"));
            var generator = f.optionalMap("generator");
            var count = f.optionalLong("count");

            var block = new ProduceBlock(to, messages, generator, count);
            block.validate();
            blocks.add(block);
        }
        return blocks;
    }

    /**
     * Parse the assert blocks from the YAML test definition.
     * @param assertArray the assert array in the YAML test definition.
     * @param streamKeys the keys of the streams map.
     * @param testKey the name of the test in the YAML test definition.
     * @param testFile the path to the test suite definition file.
     * @return a list of {@link AssertBlock}.
     */
    private List<AssertBlock> parseAssertBlocks(JsonNode assertArray, Set<String> streamKeys,
                                                String testKey, Path testFile) {
        var blocks = new ArrayList<AssertBlock>();
        for (var blockNode : assertArray) {
            var f = new FieldExtractor(blockNode, testFile);
            var on = f.optionalString("on");
            if (on != null && !streamKeys.contains(on)) {
                throw new TestDefinitionException(
                        "Assert block in test '" + testKey + "' references stream '" + on
                                + "' that is not declared in 'streams:' (" + testFile + ")");
            }
            var stores = f.optionalStringList("stores");
            var code = f.requireString("code");

            var block = new AssertBlock(on, stores, code);
            block.validate();
            blocks.add(block);
        }
        return blocks;
    }

    /**
     * Parse a list of messages from a YAML messages array.
     * @param messagesNode the messages array node in the YAML test definition.
     * @return a list of {@link TestMessage}.
     */
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

    private static String optionalText(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        return node.asText();
    }

    private static void validateIdentifier(String kind, String key, Path testFile) {
        if (!IDENTIFIER_PATTERN.matcher(key).matches()) {
            throw new TestDefinitionException(
                    "Invalid " + kind + " key '" + key + "': must match "
                            + IDENTIFIER_PATTERN.pattern()
                            + " (alphanumeric and underscore, starting with a letter) in " + testFile);
        }
    }

}
