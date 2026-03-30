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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Generates a JSON Schema for KSML test definition YAML files by reading
 * {@link JsonSchema} annotations from the record classes.
 */
public class TestDefinitionSchemaGenerator {

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
        var root = MAPPER.createObjectNode();
        root.put("$schema", "http://json-schema.org/draft-07/schema#");
        root.put("title", "KSML Test Definition");
        root.put("description", "Schema for KSML test runner YAML definition files");
        root.put("type", "object");

        // The root has a single "test" property
        var properties = root.putObject("properties");
        var testProp = properties.putObject("test");
        testProp.put("type", "object");
        testProp.put("description", annotationDescription(TestDefinition.class));
        // The YAML field name "assert" maps to the Java record component "assertions"
        buildRecordProperties(testProp, TestDefinition.class, Map.of("assertions", "assert"));

        root.putArray("required").add("test");

        return MAPPER.writeValueAsString(root);
    }

    private static void buildRecordProperties(ObjectNode parent, Class<?> recordClass) {
        buildRecordProperties(parent, recordClass, Map.of());
    }

    private static void buildRecordProperties(ObjectNode parent, Class<?> recordClass,
                                              Map<String, String> nameOverrides) {
        var properties = parent.putObject("properties");
        var required = new ArrayList<String>();

        for (var component : recordClass.getRecordComponents()) {
            var annotation = component.getAnnotation(JsonSchema.class);
            var yamlName = nameOverrides.getOrDefault(component.getName(), component.getName());
            var prop = properties.putObject(yamlName);

            setTypeForJavaType(prop, component.getType(), component.getGenericType());

            if (annotation != null) {
                if (!annotation.description().isEmpty()) {
                    prop.put("description", annotation.description());
                }
                if (!annotation.defaultValue().isEmpty()) {
                    prop.put("default", annotation.defaultValue());
                }
                if (annotation.examples().length > 0) {
                    var examples = prop.putArray("examples");
                    for (var ex : annotation.examples()) {
                        examples.add(ex);
                    }
                }
                if (annotation.required()) {
                    required.add(yamlName);
                }
            }
        }

        if (!required.isEmpty()) {
            var reqArray = parent.putArray("required");
            required.forEach(reqArray::add);
        }
    }

    private static void setTypeForJavaType(ObjectNode prop, Class<?> type, java.lang.reflect.Type genericType) {
        if (type == String.class) {
            prop.put("type", "string");
        } else if (type == Long.class || type == long.class) {
            prop.put("type", "integer");
        } else if (type == Integer.class || type == int.class) {
            prop.put("type", "integer");
        } else if (type == Boolean.class || type == boolean.class) {
            prop.put("type", "boolean");
        } else if (type == Object.class) {
            // key/value can be any type
        } else if (List.class.isAssignableFrom(type)) {
            prop.put("type", "array");
            var itemType = resolveListItemType(genericType);
            if (itemType != null && itemType.isRecord()) {
                var items = prop.putObject("items");
                items.put("type", "object");
                items.put("description", annotationDescription(itemType));
                buildRecordProperties(items, itemType);
            } else if (itemType == String.class) {
                prop.putObject("items").put("type", "string");
            }
        } else if (Map.class.isAssignableFrom(type)) {
            prop.put("type", "object");
        }
    }

    private static Class<?> resolveListItemType(java.lang.reflect.Type genericType) {
        if (genericType instanceof java.lang.reflect.ParameterizedType pt) {
            var args = pt.getActualTypeArguments();
            if (args.length == 1 && args[0] instanceof Class<?> c) {
                return c;
            }
        }
        return null;
    }

    private static String annotationDescription(Class<?> clazz) {
        var annotation = clazz.getAnnotation(JsonSchema.class);
        return annotation != null ? annotation.description() : "";
    }
}
