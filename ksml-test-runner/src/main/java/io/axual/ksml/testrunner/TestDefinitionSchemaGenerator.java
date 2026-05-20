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
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Generates a JSON Schema for KSML test suite YAML files by reflectively walking the
 * record types in this package and reading their {@link JsonSchema} annotations.
 *
 * <p>The walker is driven entirely by record metadata — per-component metadata (description,
 * defaultValue, examples, required, minProperties, yamlName) comes from the component's
 * annotation, and per-type metadata (oneOfRequired, anyOfRequired) comes from the type's
 * annotation. The only structural rule baked into this class is "{@code Map<String, X>}
 * where X is a record means a regex-keyed object" — which is the convention KSML test files
 * use for the {@code streams:} and {@code tests:} maps.
 */
public class TestDefinitionSchemaGenerator {

    /** Identifier regex shared with {@link TestDefinitionParser}. */
    static final String IDENTIFIER_REGEX = "^[a-zA-Z][a-zA-Z0-9_]*$";

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT);

    public static void main(String[] args) throws IOException {
        var schema = generateSchema();
        var outputPath = args.length > 0
                ? Path.of(args[0])
                : Path.of("src/main/resources/schemas/test-definition.schema.json");
        Files.createDirectories(outputPath.getParent());
        Files.writeString(outputPath, schema);
        System.out.println("Generated schema: " + outputPath);
    }

    static String generateSchema() throws IOException {
        var root = buildRecordSchema(TestSuiteDefinition.class);
        root.put("$schema", "http://json-schema.org/draft-07/schema#");
        root.put("title", "KSML Test Suite Definition");
        // Override the type-level description to read more like a top-level title than a record blurb
        root.put("description", "Schema for KSML test runner YAML suite definition files");
        return MAPPER.writeValueAsString(root);
    }

    /**
     * Build the JSON Schema for a record type by walking its components and reading
     * their annotations. Object-typed schemas always set {@code additionalProperties: false}.
     */
    private static ObjectNode buildRecordSchema(Class<?> recordClass) {
        var schema = MAPPER.createObjectNode();
        schema.put("type", "object");
        schema.put("additionalProperties", false);

        var typeAnnotation = recordClass.getAnnotation(JsonSchema.class);
        if (typeAnnotation != null && !typeAnnotation.description().isEmpty()) {
            schema.put("description", typeAnnotation.description());
        }

        var properties = schema.putObject("properties");
        var required = new ArrayList<String>();

        for (var component : recordClass.getRecordComponents()) {
            var componentAnnotation = component.getAnnotation(JsonSchema.class);
            var yamlName = yamlNameOf(component, componentAnnotation);
            var componentSchema = buildComponentSchema(component, componentAnnotation);
            properties.set(yamlName, componentSchema);

            if (componentAnnotation != null && componentAnnotation.required()) {
                required.add(yamlName);
            }
        }

        if (!required.isEmpty()) {
            var arr = schema.putArray("required");
            required.forEach(arr::add);
        }

        // Apply type-level structural constraints (oneOfRequired / anyOfRequired)
        if (typeAnnotation != null) {
            if (typeAnnotation.oneOfRequired().length > 0) {
                var oneOf = schema.putArray("oneOf");
                for (var fieldName : typeAnnotation.oneOfRequired()) {
                    var variant = MAPPER.createObjectNode();
                    variant.putArray("required").add(fieldName);
                    oneOf.add(variant);
                }
            }
            if (typeAnnotation.anyOfRequired().length > 0) {
                var anyOf = schema.putArray("anyOf");
                for (var fieldName : typeAnnotation.anyOfRequired()) {
                    var variant = MAPPER.createObjectNode();
                    variant.putArray("required").add(fieldName);
                    anyOf.add(variant);
                }
            }
        }

        return schema;
    }

    /**
     * Build the JSON Schema fragment for a single record component, using its annotation
     * for description/default/examples/minProperties and its Java type for shape.
     */
    private static ObjectNode buildComponentSchema(RecordComponent component, JsonSchema annotation) {
        var schema = buildTypeSchema(component.getType(), component.getGenericType());
        if (annotation != null) {
            applyComponentAnnotation(schema, annotation);
        }
        return schema;
    }

    private static ObjectNode buildTypeSchema(Class<?> rawType, Type genericType) {
        if (rawType == String.class) {
            return MAPPER.createObjectNode().put("type", "string");
        }
        if (rawType == Long.class || rawType == long.class
                || rawType == Integer.class || rawType == int.class) {
            return MAPPER.createObjectNode().put("type", "integer");
        }
        if (rawType == Boolean.class || rawType == boolean.class) {
            return MAPPER.createObjectNode().put("type", "boolean");
        }
        if (rawType == Object.class) {
            // Unconstrained — any JSON type is acceptable (used for TestMessage.key/value)
            return MAPPER.createObjectNode();
        }
        if (List.class.isAssignableFrom(rawType)) {
            var schema = MAPPER.createObjectNode().put("type", "array");
            var itemType = resolveSingleTypeArg(genericType);
            if (itemType != null) {
                schema.set("items", buildItemSchema(itemType));
            }
            return schema;
        }
        if (Map.class.isAssignableFrom(rawType)) {
            var schema = MAPPER.createObjectNode().put("type", "object");
            var valueType = resolveMapValueType(genericType);
            if (valueType instanceof Class<?> valueClass && valueClass.isRecord()) {
                // Map<String, SomeRecord> → patternProperties keyed by IDENTIFIER_REGEX
                schema.put("additionalProperties", false);
                schema.putObject("patternProperties")
                        .set(IDENTIFIER_REGEX, buildRecordSchema(valueClass));
            }
            // Otherwise Map<String, Object> — leave as plain object with no constraints
            return schema;
        }
        if (rawType.isRecord()) {
            return buildRecordSchema(rawType);
        }
        // Fallback: untyped object
        return MAPPER.createObjectNode().put("type", "object");
    }

    private static ObjectNode buildItemSchema(Type itemType) {
        if (itemType instanceof Class<?> c) {
            return buildTypeSchema(c, c);
        }
        if (itemType instanceof ParameterizedType pt && pt.getRawType() instanceof Class<?> rawClass) {
            return buildTypeSchema(rawClass, pt);
        }
        return MAPPER.createObjectNode();
    }

    private static void applyComponentAnnotation(ObjectNode schema, JsonSchema annotation) {
        if (!annotation.description().isEmpty()) {
            schema.put("description", annotation.description());
        }
        if (!annotation.defaultValue().isEmpty()) {
            schema.put("default", annotation.defaultValue());
        }
        if (annotation.examples().length > 0) {
            var examples = schema.putArray("examples");
            for (var example : annotation.examples()) {
                examples.add(example);
            }
        }
        if (annotation.minProperties() > 0) {
            schema.put("minProperties", annotation.minProperties());
        }
    }

    private static String yamlNameOf(RecordComponent component, JsonSchema annotation) {
        if (annotation != null && !annotation.yamlName().isEmpty()) {
            return annotation.yamlName();
        }
        return component.getName();
    }

    private static Type resolveSingleTypeArg(Type genericType) {
        if (genericType instanceof ParameterizedType pt) {
            var args = pt.getActualTypeArguments();
            if (args.length == 1) return args[0];
        }
        return null;
    }

    private static Type resolveMapValueType(Type genericType) {
        if (genericType instanceof ParameterizedType pt) {
            var args = pt.getActualTypeArguments();
            if (args.length == 2) return args[1];
        }
        return null;
    }
}
