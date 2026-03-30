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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class FieldExtractorTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Path TEST_FILE = Path.of("/tmp/test.yaml");

    private ObjectNode createNode() {
        return MAPPER.createObjectNode();
    }

    // requireString

    @Test
    void requireStringReturnsValue() {
        var node = createNode().put("name", "hello");
        var extractor = new FieldExtractor(node, TEST_FILE);
        assertEquals("hello", extractor.requireString("name"));
    }

    @Test
    void requireStringThrowsOnMissing() {
        var node = createNode();
        var extractor = new FieldExtractor(node, TEST_FILE);
        var ex = assertThrows(TestDefinitionException.class, () -> extractor.requireString("name"));
        assertTrue(ex.getMessage().contains("Missing required field 'name'"));
        assertTrue(ex.getMessage().contains(TEST_FILE.toString()));
    }

    @Test
    void requireStringThrowsOnNull() {
        var node = createNode();
        node.putNull("name");
        var extractor = new FieldExtractor(node, TEST_FILE);
        assertThrows(TestDefinitionException.class, () -> extractor.requireString("name"));
    }

    // optionalString

    @Test
    void optionalStringReturnsValue() {
        var node = createNode().put("color", "blue");
        var extractor = new FieldExtractor(node, TEST_FILE);
        assertEquals("blue", extractor.optionalString("color"));
    }

    @Test
    void optionalStringReturnsNullOnMissing() {
        var node = createNode();
        var extractor = new FieldExtractor(node, TEST_FILE);
        assertNull(extractor.optionalString("color"));
    }

    @Test
    void optionalStringWithDefaultReturnsValueWhenPresent() {
        var node = createNode().put("keyType", "avro:SensorData");
        var extractor = new FieldExtractor(node, TEST_FILE);
        assertEquals("avro:SensorData", extractor.optionalString("keyType", "string"));
    }

    @Test
    void optionalStringWithDefaultReturnsDefaultWhenMissing() {
        var node = createNode();
        var extractor = new FieldExtractor(node, TEST_FILE);
        assertEquals("string", extractor.optionalString("keyType", "string"));
    }

    // requireArray

    @Test
    void requireArrayReturnsArrayNode() {
        var node = createNode();
        node.putArray("items").add("a").add("b");
        var extractor = new FieldExtractor(node, TEST_FILE);
        var array = extractor.requireArray("items");
        assertEquals(2, array.size());
    }

    @Test
    void requireArrayThrowsOnMissing() {
        var node = createNode();
        var extractor = new FieldExtractor(node, TEST_FILE);
        var ex = assertThrows(TestDefinitionException.class, () -> extractor.requireArray("items"));
        assertTrue(ex.getMessage().contains("Missing or invalid 'items' array"));
    }

    @Test
    void requireArrayThrowsOnNonArray() {
        var node = createNode().put("items", "not-an-array");
        var extractor = new FieldExtractor(node, TEST_FILE);
        assertThrows(TestDefinitionException.class, () -> extractor.requireArray("items"));
    }

    // optionalLong

    @Test
    void optionalLongReturnsValue() {
        var node = createNode().put("count", 42L);
        var extractor = new FieldExtractor(node, TEST_FILE);
        assertEquals(42L, extractor.optionalLong("count"));
    }

    @Test
    void optionalLongReturnsNullOnMissing() {
        var node = createNode();
        var extractor = new FieldExtractor(node, TEST_FILE);
        assertNull(extractor.optionalLong("count"));
    }

    // optionalMap

    @Test
    void optionalMapReturnsMap() {
        var node = createNode();
        var generator = node.putObject("generator");
        generator.put("name", "gen1");
        generator.put("code", "x = 1");
        var extractor = new FieldExtractor(node, TEST_FILE);

        var result = extractor.optionalMap("generator");
        assertNotNull(result);
        assertEquals("gen1", result.get("name"));
        assertEquals("x = 1", result.get("code"));
    }

    @Test
    void optionalMapReturnsNullOnMissing() {
        var node = createNode();
        var extractor = new FieldExtractor(node, TEST_FILE);
        assertNull(extractor.optionalMap("generator"));
    }

    @Test
    void optionalMapReturnsNullOnNonObject() {
        var node = createNode().put("generator", "not-a-map");
        var extractor = new FieldExtractor(node, TEST_FILE);
        assertNull(extractor.optionalMap("generator"));
    }

    // optionalStringList

    @Test
    void optionalStringListReturnsList() {
        var node = createNode();
        node.putArray("stores").add("store1").add("store2");
        var extractor = new FieldExtractor(node, TEST_FILE);

        var result = extractor.optionalStringList("stores");
        assertEquals(List.of("store1", "store2"), result);
    }

    @Test
    void optionalStringListReturnsNullOnMissing() {
        var node = createNode();
        var extractor = new FieldExtractor(node, TEST_FILE);
        assertNull(extractor.optionalStringList("stores"));
    }

    // nodeToObject

    @Test
    void nodeToObjectConvertsString() {
        assertEquals("hello", FieldExtractor.nodeToObject(MAPPER.valueToTree("hello")));
    }

    @Test
    void nodeToObjectConvertsInt() {
        assertEquals(42, FieldExtractor.nodeToObject(MAPPER.valueToTree(42)));
    }

    @Test
    void nodeToObjectConvertsBoolean() {
        assertEquals(true, FieldExtractor.nodeToObject(MAPPER.valueToTree(true)));
    }

    @Test
    void nodeToObjectConvertsMap() {
        var result = FieldExtractor.nodeToObject(MAPPER.valueToTree(Map.of("a", "b")));
        assertInstanceOf(Map.class, result);
        assertEquals("b", ((Map<?, ?>) result).get("a"));
    }

    @Test
    void nodeToObjectConvertsArray() {
        var result = FieldExtractor.nodeToObject(MAPPER.valueToTree(List.of("a", "b")));
        assertInstanceOf(List.class, result);
        assertEquals(List.of("a", "b"), result);
    }

    @Test
    void nodeToObjectReturnsNullForNull() {
        assertNull(FieldExtractor.nodeToObject(null));
    }
}
