package io.axual.ksml.notation.binary;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2023 Axual B.V.
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
import io.axual.ksml.data.value.Tuple;
import io.axual.ksml.exception.KSMLExecutionException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonNodeNativeMapper {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public JsonNode fromNative(Object value) {
        if (value instanceof List<?> list)
            return fromNativeList(list);
        if (value instanceof Map<?, ?> map)
            return fromNativeMap(map);
        throw new KSMLExecutionException("Can not convert to JsonNode: " + (value != null ? value.getClass().getSimpleName() : "null"));
    }

    private JsonNode fromNativeList(List<?> list) {
        var result = MAPPER.createArrayNode();
        for (Object element : list) {
            addToArrayNode(result, element);
        }
        return result;
    }

    private JsonNode fromNativeMap(Map<?, ?> map) {
        var result = MAPPER.createObjectNode();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            var key = entry.getKey().toString();
            var value = fromNativeValue(entry.getValue());
            addToObjectNode(result, key, value);
        }
        return result;
    }

    private JsonNode tupleToJsonNode(Tuple<?> value) {
        var result = MAPPER.createArrayNode();
        for (Object element : value.elements()) {
            addToArrayNode(result, fromNativeValue(element));
        }
        return result;
    }

    private Object fromNativeValue(Object value) {
        if (value instanceof List<?>) return fromNative(value);
        if (value instanceof Map<?, ?>) return fromNative(value);
        return value;
    }

    private JsonNode addToArrayNode(ArrayNode node, Object value) {
        if (value == null) return node.add(MAPPER.nullNode());
        if (value instanceof Byte val) return node.add(val);
        if (value instanceof Short val) return node.add(val);
        if (value instanceof Integer val) return node.add(val);
        if (value instanceof Long val) return node.add(val);
        if (value instanceof Double val) return node.add(val);
        if (value instanceof Float val) return node.add(val);
        if (value instanceof byte[] val) return node.add(val);
        if (value instanceof String val) return node.add(val);
        if (value instanceof JsonNode val) return node.add(val);
        if (value instanceof Tuple<?> val) return node.add(tupleToJsonNode(val));
        if (value instanceof List<?> || value instanceof Map<?, ?>)
            return node.add(fromNative(value));
        throw new KSMLExecutionException("Can not add value to ObjectNode: " + value.getClass().getSimpleName());
    }

    private JsonNode addToObjectNode(ObjectNode node, String key, Object value) {
        if (value == null) return node.set(key, MAPPER.nullNode());
        if (value instanceof Byte val) return node.put(key, val);
        if (value instanceof Short val) return node.put(key, val);
        if (value instanceof Integer val) return node.put(key, val);
        if (value instanceof Long val) return node.put(key, val);
        if (value instanceof Double val) return node.put(key, val);
        if (value instanceof Float val) return node.put(key, val);
        if (value instanceof byte[] val) return node.put(key, val);
        if (value instanceof String val) return node.put(key, val);
        if (value instanceof JsonNode val) return node.set(key, val);
        if (value instanceof Tuple<?> val) return node.set(key, tupleToJsonNode(val));
        if (value instanceof List<?> || value instanceof Map<?, ?>)
            node.set(key, fromNative(value));
        throw new KSMLExecutionException("Can not add value to ObjectNode: " + value.getClass().getSimpleName());
    }

    public Object toNative(JsonNode node) {
        if (node.isArray()) return toNativeList((ArrayNode) node);
        if (node.isObject()) return toNativeMap((ObjectNode) node);
        throw new KSMLExecutionException("Can not convert from JsonNode: " + node.toPrettyString());
    }

    private Object toNativeList(ArrayNode node) {
        var result = new ArrayList<>();
        for (var it = node.elements(); it.hasNext(); ) {
            var element = it.next();
            result.add(toNativeValue(element));
        }
        return result;
    }

    private Object toNativeMap(ObjectNode node) {
        var result = new HashMap<String, Object>();
        for (var it = node.fields(); it.hasNext(); ) {
            var entry = it.next();
            result.put(entry.getKey(), toNativeValue(entry.getValue()));
        }
        return result;
    }

    private Object toNativeValue(JsonNode value) {
        try {
            if (value.isNull()) return null;
            if (value.isShort()) return value.shortValue();
            if (value.isInt()) return value.intValue();
            if (value.isLong()) return value.longValue();
            if (value.isDouble()) return value.doubleValue();
            if (value.isFloat()) return value.floatValue();
            if (value.isBinary()) return value.binaryValue();
            if (value.isTextual()) return value.textValue();
            if (value.isArray()) return toNative(value);
            if (value.isObject()) return toNative(value);
        } catch (Exception e) {
            throw new KSMLExecutionException("Can not convert from JSON value: " + value.getClass().getSimpleName(), e);
        }
        throw new KSMLExecutionException("Can not convert from JSON value: " + value.getClass().getSimpleName());
    }
}
