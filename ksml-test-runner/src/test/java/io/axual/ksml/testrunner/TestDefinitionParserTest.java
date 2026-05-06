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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class TestDefinitionParserTest {

    private final TestDefinitionParser parser = new TestDefinitionParser();

    private Path resource(String name) {
        var url = getClass().getClassLoader().getResource(name);
        assertNotNull(url, "Test resource not found: " + name);
        return Path.of(url.getPath());
    }

    @Test
    void parsesValidSuiteDefinition() throws IOException {
        var suite = parser.parse(resource("valid-test-definition.yaml"));

        assertEquals("Filter keeps only blue sensors", suite.name());
        assertEquals("pipelines/test-filter.yaml", suite.pipeline());
        assertEquals("schemas", suite.schemaDirectory());

        // Streams
        assertEquals(2, suite.streams().size());
        assertTrue(suite.streams().containsKey("sensor_source"));
        assertTrue(suite.streams().containsKey("sensor_filtered"));
        assertEquals("ksml_sensordata_avro", suite.streams().get("sensor_source").topic());
        assertEquals("avro:SensorData", suite.streams().get("sensor_source").valueType());

        // Tests
        assertEquals(1, suite.tests().size());
        var testCase = suite.tests().get("blue_sensors_pass");
        assertNotNull(testCase);

        // Produce block references stream by name
        assertEquals(1, testCase.produce().size());
        var produce = testCase.produce().getFirst();
        assertEquals("sensor_source", produce.to());
        assertEquals(3, produce.messages().size());
        assertNull(produce.generator());

        // First message: no timestamp
        var msg1 = produce.messages().getFirst();
        assertEquals("sensor1", msg1.key());
        assertInstanceOf(Map.class, msg1.value());
        assertNull(msg1.timestamp());

        // Third message: has timestamp
        var msg3 = produce.messages().get(2);
        assertEquals(1000L, msg3.timestamp());

        // Assert blocks
        assertEquals(1, testCase.assertions().size());
        var assertBlock = testCase.assertions().getFirst();
        assertEquals("sensor_filtered", assertBlock.on());
        assertNull(assertBlock.stores());
        assertNotNull(assertBlock.code());
        assertTrue(assertBlock.code().contains("assert len(records) == 2"));
    }

    @Test
    void parsesTestWithStoreAssertions() throws IOException {
        var suite = parser.parse(resource("valid-test-with-stores.yaml"));

        assertEquals("State store tracks last sensor reading", suite.name());

        var testCase = suite.tests().values().iterator().next();
        var assertBlock = testCase.assertions().getFirst();
        assertNull(assertBlock.on());
        assertNotNull(assertBlock.stores());
        assertEquals(1, assertBlock.stores().size());
        assertEquals("last_sensor_data_store", assertBlock.stores().getFirst());
    }

    @Test
    void rejectsMissingTests() {
        var ex = assertThrows(TestDefinitionException.class,
                () -> parser.parse(resource("missing-tests.yaml")));
        assertTrue(ex.getMessage().contains("Missing required 'tests' map")
                        || ex.getMessage().contains("at least one entry"),
                "expected missing-tests message but got: " + ex.getMessage());
    }

    @Test
    void rejectsMissingProduce() {
        var ex = assertThrows(TestDefinitionException.class,
                () -> parser.parse(resource("missing-produce.yaml")));
        assertTrue(ex.getMessage().contains("missing required field 'produce'"),
                "expected missing-produce message but got: " + ex.getMessage());
    }

    @Test
    void rejectsAssertWithoutOnOrStores() {
        var ex = assertThrows(TestDefinitionException.class,
                () -> parser.parse(resource("assert-no-topic-no-stores.yaml")));
        assertTrue(ex.getMessage().contains("must have at least 'on' or 'stores'"),
                "expected on-or-stores message but got: " + ex.getMessage());
    }

    @Test
    void rejectsInvalidTestKey() {
        var ex = assertThrows(TestDefinitionException.class,
                () -> parser.parse(resource("invalid-test-key.yaml")));
        assertTrue(ex.getMessage().contains("Invalid test key"),
                "expected invalid-test-key message but got: " + ex.getMessage());
    }

    @Test
    void rejectsInvalidStreamKey() {
        var ex = assertThrows(TestDefinitionException.class,
                () -> parser.parse(resource("invalid-stream-key.yaml")));
        assertTrue(ex.getMessage().contains("Invalid stream key"),
                "expected invalid-stream-key message but got: " + ex.getMessage());
    }

    @Test
    void rejectsDuplicateTestKey() {
        var ex = assertThrows(TestDefinitionException.class,
                () -> parser.parse(resource("duplicate-test-key.yaml")));
        assertTrue(ex.getMessage().contains("Duplicate"),
                "expected Duplicate message but got: " + ex.getMessage());
    }

    @Test
    void rejectsDuplicateStreamTopic() {
        var ex = assertThrows(TestDefinitionException.class,
                () -> parser.parse(resource("duplicate-stream-topic.yaml")));
        assertTrue(ex.getMessage().contains("both reference topic"),
                "expected duplicate-topic message but got: " + ex.getMessage());
    }

    @Test
    void rejectsUndefinedStreamReference() {
        var ex = assertThrows(TestDefinitionException.class,
                () -> parser.parse(resource("undefined-stream-reference.yaml")));
        assertTrue(ex.getMessage().contains("not declared in 'streams:'"),
                "expected undefined-reference message but got: " + ex.getMessage());
    }

    // Note: rejection of bare schema-bearing notations (e.g., 'confluent_avro' without a schema
    // name) and other UserTypeParser-driven errors happen at runtime, not parse time, because the
    // parser does not depend on the global notation library. End-to-end coverage lives in
    // KSMLTestRunnerTest.invalidDefinitionsReturnNonPass.

    @Test
    void parsesMessageValueAsMap() throws IOException {
        var suite = parser.parse(resource("valid-test-definition.yaml"));
        var testCase = suite.tests().get("blue_sensors_pass");
        var value = testCase.produce().getFirst().messages().getFirst().value();
        assertInstanceOf(Map.class, value);

        @SuppressWarnings("unchecked")
        var map = (Map<String, Object>) value;
        assertEquals("sensor1", map.get("name"));
        assertEquals("blue", map.get("color"));
        assertEquals("Amsterdam", map.get("city"));
    }

    @Test
    void parsesOptionalSchemaDirectory() throws IOException {
        var suite = parser.parse(resource("valid-test-with-stores.yaml"));
        assertEquals("schemas", suite.schemaDirectory());
    }

    @Test
    void omittedKeyTypeAndValueTypeDefaultToString() throws IOException {
        var suite = parser.parse(resource("defaults-test.yaml"));

        var inputStream = suite.streams().get("input");
        assertEquals("string", inputStream.keyType());
        assertEquals("string", inputStream.valueType());
    }

    @Test
    void buildTopicTypeMapKeysByTopic() throws IOException {
        var suite = parser.parse(resource("sample-filter-test-confluent-avro.yaml"));
        var map = TestDefinitionParser.buildTopicTypeMap(suite);

        assertTrue(map.containsKey("ksml_sensordata_avro"));
        assertTrue(map.containsKey("ksml_sensordata_filtered"));

        var inputEntry = map.get("ksml_sensordata_avro");
        assertEquals("string", inputEntry.keyType());
        assertEquals("avro:SensorData", inputEntry.valueType());
    }
}
