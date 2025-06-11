package io.axual.ksml.data.util;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library
 * %%
 * Copyright (C) 2021 - 2025 Axual B.V.
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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.value.Tuple;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonNodeUtil {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Nullable
    public static List<Object> convertStringToList(String value) {
        final var tree = convertStringToJsonNode(value);
        return tree != null && tree.isArray() ? convertArrayNodeToList((ArrayNode) tree) : null;
    }

    @Nullable
    public static Map<String, Object> convertStringToMap(String value) {
        final var tree = convertStringToJsonNode(value);
        return tree != null && tree.isObject() ? convertObjectNodeToMap((ObjectNode) tree) : null;
    }

    @Nullable
    public static JsonNode convertStringToJsonNode(String value) {
        if (value == null) return null; // Allow null strings as input, returning null as native output
        try {
            return OBJECT_MAPPER.readTree(value);
        } catch (Exception mapException) {
            // Ignore
        }
        return null;
    }

    public static JsonNode convertNativeToJsonNode(Object value) {
        if (value instanceof List<?> list)
            return convertListToJsonNode(list);
        if (value instanceof Map<?, ?> map)
            return convertMapToJsonNode(map);
        throw new DataException("Can not convert to JsonNode: " + (value != null ? value.getClass().getSimpleName() : "null"));
    }

    private static JsonNode convertListToJsonNode(List<?> list) {
        final var result = OBJECT_MAPPER.createArrayNode();
        for (Object element : list) {
            addValueToArrayNode(result, element);
        }
        return result;
    }

    private static JsonNode convertMapToJsonNode(Map<?, ?> map) {
        final var result = OBJECT_MAPPER.createObjectNode();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            final var key = entry.getKey().toString();
            final var value = convertValueToJsonNode(entry.getValue());
            addKeyValueToObjectNode(result, key, value);
        }
        return result;
    }

    private static JsonNode convertTupleToJsonNode(Tuple<?> value) {
        final var result = OBJECT_MAPPER.createArrayNode();
        for (Object element : value.elements())
            addValueToArrayNode(result, convertValueToJsonNode(element));
        return result;
    }

    private static Object convertValueToJsonNode(Object value) {
        if (value instanceof List<?>) return convertNativeToJsonNode(value);
        if (value instanceof Map<?, ?>) return convertNativeToJsonNode(value);
        return value;
    }

    private static JsonNode addValueToArrayNode(ArrayNode node, Object value) {
        if (value == null) return node.add(OBJECT_MAPPER.nullNode());
        if (value instanceof Boolean val) return node.add(val);
        if (value instanceof Byte val) return node.add(val);
        if (value instanceof Short val) return node.add(val);
        if (value instanceof Integer val) return node.add(val);
        if (value instanceof Long val) return node.add(val);
        if (value instanceof Double val) return node.add(val);
        if (value instanceof Float val) return node.add(val);
        if (value instanceof byte[] val) return node.add(val);
        if (value instanceof String val) return node.add(val);
        if (value instanceof JsonNode val) return node.add(val);
        if (value instanceof Tuple<?> val) return node.add(convertTupleToJsonNode(val));
        if (value instanceof List<?> || value instanceof Map<?, ?>)
            return node.add(convertNativeToJsonNode(value));
        throw new DataException("Can not add value to ObjectNode: " + value.getClass().getSimpleName());
    }

    private static JsonNode addKeyValueToObjectNode(ObjectNode node, String key, Object value) {
        if (value == null) return node.set(key, OBJECT_MAPPER.nullNode());
        if (value instanceof Boolean val) return node.put(key, val);
        if (value instanceof Byte val) return node.put(key, val);
        if (value instanceof Short val) return node.put(key, val);
        if (value instanceof Integer val) return node.put(key, val);
        if (value instanceof Long val) return node.put(key, val);
        if (value instanceof Double val) return node.put(key, val);
        if (value instanceof Float val) return node.put(key, val);
        if (value instanceof byte[] val) return node.put(key, val);
        if (value instanceof String val) return node.put(key, val);
        if (value instanceof JsonNode val) return node.set(key, val);
        if (value instanceof Tuple<?> val) return node.set(key, convertTupleToJsonNode(val));
        if (value instanceof List<?> || value instanceof Map<?, ?>)
            node.set(key, convertNativeToJsonNode(value));
        throw new DataException("Can not add value to ObjectNode: " + value.getClass().getSimpleName());
    }

    public static Object convertJsonNodeToNative(JsonNode node) {
        if (node.isArray()) return convertArrayNodeToList((ArrayNode) node);
        if (node.isObject()) return convertObjectNodeToMap((ObjectNode) node);
        throw new DataException("Can not convert from JsonNode: " + node.toPrettyString());
    }

    private static List<Object> convertArrayNodeToList(ArrayNode node) {
        final var result = new ArrayList<>();
        for (final var it = node.elements(); it.hasNext(); ) {
            final var element = it.next();
            result.add(convertJsonNodeValueToNative(element));
        }
        return result;
    }

    private static Map<String, Object> convertObjectNodeToMap(ObjectNode node) {
        final var result = new HashMap<String, Object>();
        for (final Map.Entry<String, JsonNode> entry : node.properties()) {
            result.put(entry.getKey(), convertJsonNodeValueToNative(entry.getValue()));
        }
        return result;
    }

    private static Object convertJsonNodeValueToNative(JsonNode value) {
        try {
            if (value.isNull()) return null;
            if (value.isBoolean()) return value.booleanValue();
            if (value.isShort()) return value.shortValue();
            if (value.isInt()) return value.intValue();
            if (value.isLong()) return value.longValue();
            if (value.isDouble()) return value.doubleValue();
            if (value.isFloat()) return value.floatValue();
            if (value.isBinary()) return value.binaryValue();
            if (value.isTextual()) return value.textValue();
            if (value.isArray()) return convertJsonNodeToNative(value);
            if (value.isObject()) return convertJsonNodeToNative(value);
        } catch (Exception e) {
            throw new DataException("Can not convert from JSON value: " + value.getClass().getSimpleName(), e);
        }
        throw new DataException("Can not convert from JSON value: " + value.getClass().getSimpleName());
    }

    private JsonNodeUtil() {
        // Prevent instantiation
    }
}
