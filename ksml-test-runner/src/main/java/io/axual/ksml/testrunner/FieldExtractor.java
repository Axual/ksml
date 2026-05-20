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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility for extracting typed fields from a {@link JsonNode} with rich error messages.
 * All error messages include the file path for context.
 */
class FieldExtractor {

    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

    private final JsonNode node;
    private final Path contextFile;

    FieldExtractor(JsonNode node, Path contextFile) {
        this.node = node;
        this.contextFile = contextFile;
    }

    /**
     * Get a required string field. Throws if missing or null.
     */
    String requireString(String field) {
        var child = node.get(field);
        if (child == null || child.isNull()) {
            throw new TestDefinitionException("Missing required field '" + field + "' in " + contextFile);
        }
        return child.asText();
    }

    /**
     * Get an optional string field, returning null if absent.
     */
    String optionalString(String field) {
        var child = node.get(field);
        if (child == null || child.isNull()) {
            return null;
        }
        return child.asText();
    }

    /**
     * Get an optional string field with a default value.
     */
    String optionalString(String field, String defaultValue) {
        var result = optionalString(field);
        return result != null ? result : defaultValue;
    }

    /**
     * Get a required array field. Throws if missing or not an array.
     */
    JsonNode requireArray(String field) {
        var child = node.get(field);
        if (child == null || !child.isArray()) {
            throw new TestDefinitionException("Missing or invalid '" + field + "' array in " + contextFile);
        }
        return child;
    }

    /**
     * Get an optional long field, returning null if absent.
     */
    Long optionalLong(String field) {
        if (!node.has(field)) {
            return null;
        }
        return node.get(field).asLong();
    }

    /**
     * Get an optional object field as a map, returning null if absent.
     */
    @SuppressWarnings("unchecked")
    Map<String, Object> optionalMap(String field) {
        var child = node.get(field);
        if (child == null || !child.isObject()) {
            return null;
        }
        return YAML_MAPPER.convertValue(child, LinkedHashMap.class);
    }

    /**
     * Get an optional array field as a list of strings, returning null if absent.
     */
    List<String> optionalStringList(String field) {
        var child = node.get(field);
        if (child == null || !child.isArray()) {
            return null;
        }
        var result = new ArrayList<String>();
        for (var element : child) {
            result.add(element.asText());
        }
        return result;
    }

    /**
     * Convert a JsonNode value to a native Java object (String, Integer, Map, List, etc.).
     */
    static Object nodeToObject(JsonNode jsonNode) {
        if (jsonNode == null || jsonNode.isNull()) {
            return null;
        }
        if (jsonNode.isTextual()) {
            return jsonNode.asText();
        }
        if (jsonNode.isNumber()) {
            if (jsonNode.isInt()) return jsonNode.asInt();
            if (jsonNode.isLong()) return jsonNode.asLong();
            return jsonNode.asDouble();
        }
        if (jsonNode.isBoolean()) {
            return jsonNode.asBoolean();
        }
        if (jsonNode.isObject()) {
            return YAML_MAPPER.convertValue(jsonNode, LinkedHashMap.class);
        }
        if (jsonNode.isArray()) {
            var list = new ArrayList<>();
            for (var item : jsonNode) {
                list.add(nodeToObject(item));
            }
            return list;
        }
        return jsonNode.asText();
    }
}
