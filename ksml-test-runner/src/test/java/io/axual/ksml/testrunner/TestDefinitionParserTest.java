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
    void parsesValidTestDefinition() throws IOException {
        var def = parser.parse(resource("valid-test-definition.yaml"));

        assertEquals("Filter keeps only blue sensors", def.name());
        assertEquals("pipelines/test-filter.yaml", def.pipeline());
        assertEquals("schemas", def.schemaDirectory());

        // Produce blocks
        assertEquals(1, def.produce().size());
        var produce = def.produce().getFirst();
        assertEquals("ksml_sensordata_avro", produce.topic());
        assertEquals("string", produce.keyType());
        assertEquals("avro:SensorData", produce.valueType());
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
        assertEquals(1, def.assertions().size());
        var assertBlock = def.assertions().getFirst();
        assertEquals("ksml_sensordata_filtered", assertBlock.topic());
        assertNull(assertBlock.stores());
        assertNotNull(assertBlock.code());
        assertTrue(assertBlock.code().contains("assert len(records) == 2"));
    }

    @Test
    void parsesTestWithStoreAssertions() throws IOException {
        var def = parser.parse(resource("valid-test-with-stores.yaml"));

        assertEquals("State store tracks last sensor reading", def.name());

        var assertBlock = def.assertions().getFirst();
        assertNull(assertBlock.topic());
        assertNotNull(assertBlock.stores());
        assertEquals(1, assertBlock.stores().size());
        assertEquals("last_sensor_data_store", assertBlock.stores().getFirst());
    }

    @Test
    void rejectsMissingTestRoot() {
        var ex = assertThrows(TestDefinitionException.class,
                () -> parser.parse(resource("missing-test-root.yaml")));
        assertTrue(ex.getMessage().contains("Missing required 'test' root element"));
    }

    @Test
    void rejectsMissingName() {
        var ex = assertThrows(TestDefinitionException.class,
                () -> parser.parse(resource("missing-name.yaml")));
        assertTrue(ex.getMessage().contains("Missing required field 'name'"));
    }

    @Test
    void rejectsMissingProduce() {
        var ex = assertThrows(TestDefinitionException.class,
                () -> parser.parse(resource("missing-produce.yaml")));
        assertTrue(ex.getMessage().contains("Missing or invalid 'produce' array"));
    }

    @Test
    void rejectsAssertWithoutTopicOrStores() {
        var ex = assertThrows(TestDefinitionException.class,
                () -> parser.parse(resource("assert-no-topic-no-stores.yaml")));
        assertTrue(ex.getMessage().contains("must have at least 'topic' or 'stores'"));
    }

    @Test
    void parsesMessageValueAsMap() throws IOException {
        var def = parser.parse(resource("valid-test-definition.yaml"));
        var value = def.produce().getFirst().messages().getFirst().value();
        assertInstanceOf(Map.class, value);

        @SuppressWarnings("unchecked")
        var map = (Map<String, Object>) value;
        assertEquals("sensor1", map.get("name"));
        assertEquals("blue", map.get("color"));
        assertEquals("Amsterdam", map.get("city"));
    }

    @Test
    void parsesOptionalSchemaDirectoryAsNull() throws IOException {
        var def = parser.parse(resource("valid-test-with-stores.yaml"));
        // This file has a schemaDirectory
        assertEquals("schemas", def.schemaDirectory());
    }

    @Test
    void omittedKeyTypeAndValueTypeAreNull() throws IOException {
        var def = parser.parse(resource("defaults-test.yaml"));

        var produce = def.produce().getFirst();
        assertNull(produce.keyType());
        assertNull(produce.valueType());
    }

    @Test
    void explicitTypesOverrideDefaults() throws IOException {
        var def = parser.parse(resource("valid-test-definition.yaml"));

        var produce = def.produce().getFirst();
        assertEquals("string", produce.keyType());
        assertEquals("avro:SensorData", produce.valueType());
    }

    @Test
    void parsesRegistryBlock() throws IOException {
        var def = parser.parse(resource("sample-filter-test-confluent-avro.yaml"));

        assertNotNull(def.registry());
        assertEquals(2, def.registry().size());

        var entry1 = def.registry().getFirst();
        assertEquals("ksml_sensordata_avro", entry1.topic());
        assertEquals("string", entry1.keyType());
        assertEquals("avro:SensorData", entry1.valueType());

        var entry2 = def.registry().get(1);
        assertEquals("ksml_sensordata_filtered", entry2.topic());
    }

    @Test
    void registryIsNullWhenOmitted() throws IOException {
        var def = parser.parse(resource("valid-test-definition.yaml"));
        assertNull(def.registry());
    }

    @Test
    void buildTopicTypeMapMergesRegistryAndProduceBlocks() throws IOException {
        var def = parser.parse(resource("sample-filter-test-confluent-avro.yaml"));
        var map = TestDefinitionParser.buildTopicTypeMap(def);

        // Both topics from registry should be present
        assertTrue(map.containsKey("ksml_sensordata_avro"));
        assertTrue(map.containsKey("ksml_sensordata_filtered"));

        // Produce block has explicit types that match registry, so produce wins
        var inputEntry = map.get("ksml_sensordata_avro");
        assertEquals("string", inputEntry.keyType());
        assertEquals("avro:SensorData", inputEntry.valueType());
    }

    @Test
    void buildTopicTypeMapUsesRegistryWhenProduceOmitsTypes() throws IOException {
        var def = parser.parse(resource("sample-filter-test-registry-only-types.yaml"));
        var map = TestDefinitionParser.buildTopicTypeMap(def);

        // Input topic: produce block has no types, so registry entry is used
        var inputEntry = map.get("ksml_sensordata_avro");
        assertEquals("string", inputEntry.keyType());
        assertEquals("avro:SensorData", inputEntry.valueType());

        // Output topic: only in registry
        var outputEntry = map.get("ksml_sensordata_filtered");
        assertEquals("avro:SensorData", outputEntry.valueType());
    }
}
