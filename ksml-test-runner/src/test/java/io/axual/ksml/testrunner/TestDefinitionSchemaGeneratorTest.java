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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Structural tests for {@link TestDefinitionSchemaGenerator}.
 *
 * <p>These tests parse the generated JSON Schema back into a {@link JsonNode} tree and assert on
 * its shape (Level 1) and on values that should flow from {@link JsonSchema} annotations on the
 * record classes (Level 2). They guard against regressions in the reflection logic — e.g. if the
 * generator stops honoring {@code yamlName}, or drops {@code additionalProperties: false}, or
 * forgets to emit the type-level {@code oneOfRequired}/{@code anyOfRequired} constraints.
 *
 * <p>True JSON Schema validation of fixture files is intentionally out of scope here;
 * see the design document for the planned follow-up.
 */
class TestDefinitionSchemaGeneratorTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // JSON Pointer path to the per-stream sub-schema and per-test sub-schema under their
    // respective patternProperties maps. The identifier regex contains no `/` or `~` so it
    // is safe to use as a JSON Pointer path segment as-is.
    private static final String STREAM_SCHEMA_POINTER =
            "/properties/streams/patternProperties/" + TestDefinitionSchemaGenerator.IDENTIFIER_REGEX;
    private static final String TEST_SCHEMA_POINTER =
            "/properties/tests/patternProperties/" + TestDefinitionSchemaGenerator.IDENTIFIER_REGEX;

    private static JsonNode root;

    @BeforeAll
    static void generate() throws IOException {
        root = MAPPER.readTree(TestDefinitionSchemaGenerator.generateSchema());
    }

    // ---------------------------------------------------------------------------------------
    // Level 1: structural shape
    // ---------------------------------------------------------------------------------------

    @Test
    void rootIsAnObjectWithJsonSchemaPreamble() {
        assertEquals("http://json-schema.org/draft-07/schema#", root.get("$schema").asText());
        assertEquals("KSML Test Suite Definition", root.get("title").asText());
        assertEquals("object", root.get("type").asText());
        assertFalse(root.get("additionalProperties").asBoolean(),
                "Root object schema must close down extra properties");
    }

    @Test
    void rootRequiresDefinitionAndTests() {
        var required = root.get("required");
        assertNotNull(required, "Root must declare a required array");
        var names = new java.util.HashSet<String>();
        required.forEach(n -> names.add(n.asText()));
        assertEquals(java.util.Set.of("definition", "tests"), names);
    }

    @Test
    void rootDeclaresAllExpectedTopLevelProperties() {
        var props = root.get("properties");
        for (var field : java.util.List.of("name", "definition", "schemaDirectory",
                "moduleDirectory", "streams", "tests")) {
            assertTrue(props.has(field), "Missing top-level property '" + field + "'");
        }
    }

    @Test
    void streamsUsesPatternPropertiesKeyedByIdentifierRegex() {
        var streams = root.path("properties").path("streams");
        assertEquals("object", streams.get("type").asText());
        assertFalse(streams.get("additionalProperties").asBoolean());
        var pattern = streams.path("patternProperties");
        assertTrue(pattern.has(TestDefinitionSchemaGenerator.IDENTIFIER_REGEX),
                "streams.patternProperties must be keyed by the identifier regex");
    }

    @Test
    void testsUsesPatternPropertiesAndRequiresAtLeastOneEntry() {
        var tests = root.path("properties").path("tests");
        assertEquals("object", tests.get("type").asText());
        assertFalse(tests.get("additionalProperties").asBoolean());
        assertEquals(1, tests.get("minProperties").asInt(),
                "tests map must require at least one entry");
        assertTrue(tests.path("patternProperties").has(TestDefinitionSchemaGenerator.IDENTIFIER_REGEX));
    }

    @Test
    void streamSchemaRequiresTopicAndDefaultsKeyValueTypeToString() {
        var streamSchema = root.at(STREAM_SCHEMA_POINTER);
        assertEquals("object", streamSchema.get("type").asText());
        assertFalse(streamSchema.get("additionalProperties").asBoolean());

        var required = streamSchema.get("required");
        assertEquals(1, required.size());
        assertEquals("topic", required.get(0).asText());

        assertEquals("string", streamSchema.at("/properties/keyType/default").asText());
        assertEquals("string", streamSchema.at("/properties/valueType/default").asText());
    }

    @Test
    void testCaseSchemaRequiresProduceAndAssert() {
        var testSchema = root.at(TEST_SCHEMA_POINTER);
        assertEquals("object", testSchema.get("type").asText());
        var required = new java.util.HashSet<String>();
        testSchema.get("required").forEach(n -> required.add(n.asText()));
        assertEquals(java.util.Set.of("produce", "assert"), required);
    }

    @Test
    void produceBlockOneOfBetweenMessagesAndGenerator() {
        var produceItem = root.at(TEST_SCHEMA_POINTER + "/properties/produce/items");
        assertEquals("object", produceItem.get("type").asText());

        var oneOf = produceItem.get("oneOf");
        assertNotNull(oneOf, "Produce block schema must declare oneOf");
        var variantRequired = new java.util.HashSet<String>();
        for (var variant : oneOf) {
            variant.get("required").forEach(n -> variantRequired.add(n.asText()));
        }
        assertEquals(java.util.Set.of("messages", "generator"), variantRequired);

        // 'to' is itself required regardless of which variant is chosen
        assertEquals("to", produceItem.get("required").get(0).asText());
    }

    @Test
    void assertBlockAnyOfBetweenOnAndStores() {
        var assertItem = root.at(TEST_SCHEMA_POINTER + "/properties/assert/items");
        assertEquals("object", assertItem.get("type").asText());

        var anyOf = assertItem.get("anyOf");
        assertNotNull(anyOf, "Assert block schema must declare anyOf");
        var variantRequired = new java.util.HashSet<String>();
        for (var variant : anyOf) {
            variant.get("required").forEach(n -> variantRequired.add(n.asText()));
        }
        assertEquals(java.util.Set.of("on", "stores"), variantRequired);

        // 'code' is required unconditionally
        assertEquals("code", assertItem.get("required").get(0).asText());
    }

    @Test
    void assertionsRecordComponentIsRenamedToAssertInYaml() {
        var testSchema = root.at(TEST_SCHEMA_POINTER);
        assertTrue(testSchema.path("properties").has("assert"),
                "yamlName override must rename 'assertions' to 'assert'");
        assertFalse(testSchema.path("properties").has("assertions"),
                "Java component name 'assertions' must not leak into the schema");
    }

    // ---------------------------------------------------------------------------------------
    // Level 2: annotation values flow into the generated schema
    // ---------------------------------------------------------------------------------------

    @Test
    void definitionFieldCarriesAnnotationDescription() {
        var annotation = componentAnnotation(TestSuiteDefinition.class, "definition");
        var schemaDescription = root.at("/properties/definition/description").asText();
        assertEquals(annotation.description(), schemaDescription,
                "definition field description must come from the @JsonSchema annotation");
    }

    @Test
    void definitionFieldCarriesAnnotationExamples() {
        var annotation = componentAnnotation(TestSuiteDefinition.class, "definition");
        var examples = root.at("/properties/definition/examples");
        assertEquals(annotation.examples().length, examples.size());
        for (int i = 0; i < annotation.examples().length; i++) {
            assertEquals(annotation.examples()[i], examples.get(i).asText());
        }
    }

    @Test
    void streamTopicFieldCarriesAnnotationDescription() {
        var annotation = componentAnnotation(StreamDefinition.class, "topic");
        var schemaDescription = root.at(STREAM_SCHEMA_POINTER + "/properties/topic/description").asText();
        assertEquals(annotation.description(), schemaDescription);
    }

    @Test
    void testCaseDescriptionFieldCarriesAnnotationDescription() {
        var annotation = componentAnnotation(TestCaseDefinition.class, "description");
        var schemaDescription = root.at(TEST_SCHEMA_POINTER + "/properties/description/description").asText();
        assertEquals(annotation.description(), schemaDescription);
    }

    @Test
    void typeLevelDescriptionsFromAnnotationsAppearOnRecordSchemas() {
        var streamTypeDescription = StreamDefinition.class.getAnnotation(JsonSchema.class).description();
        var testCaseTypeDescription = TestCaseDefinition.class.getAnnotation(JsonSchema.class).description();
        var produceTypeDescription = ProduceBlock.class.getAnnotation(JsonSchema.class).description();
        var assertTypeDescription = AssertBlock.class.getAnnotation(JsonSchema.class).description();

        assertEquals(streamTypeDescription, root.at(STREAM_SCHEMA_POINTER + "/description").asText());
        assertEquals(testCaseTypeDescription, root.at(TEST_SCHEMA_POINTER + "/description").asText());
        assertEquals(produceTypeDescription,
                root.at(TEST_SCHEMA_POINTER + "/properties/produce/items/description").asText());
        assertEquals(assertTypeDescription,
                root.at(TEST_SCHEMA_POINTER + "/properties/assert/items/description").asText());
    }

    // ---------------------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------------------

    private static JsonSchema componentAnnotation(Class<?> recordClass, String componentName) {
        for (var component : recordClass.getRecordComponents()) {
            if (component.getName().equals(componentName)) {
                var annotation = component.getAnnotation(JsonSchema.class);
                assertNotNull(annotation, "Expected @JsonSchema on " + recordClass.getSimpleName()
                        + "." + componentName);
                return annotation;
            }
        }
        throw new AssertionError("No record component named '" + componentName + "' on "
                + recordClass.getSimpleName());
    }
}
