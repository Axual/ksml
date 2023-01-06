package io.axual.ksml.data.mapper;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2022 Axual B.V.
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.axual.ksml.data.value.Tuple;
import io.axual.ksml.exception.KSMLDataException;
import io.axual.ksml.notation.json.JsonNodeMapper;

public class NativeJsonNodeMapper implements JsonNodeMapper<Object> {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public JsonNode toJsonNode(Object value) {
        if (value instanceof List<?> list)
            return listToJsonNode(list);
        if (value instanceof Map<?, ?> map)
            return mapToJsonNode(map);
        throw new KSMLDataException("Can not convert to JsonNode: " + value.getClass().getSimpleName());
    }

    private JsonNode listToJsonNode(List<?> list) {
        var result = MAPPER.createArrayNode();
        for (Object element : list) {
            result.add(toJsonNode(element));
        }
        return result;
    }

    private JsonNode mapToJsonNode(Map<?, ?> map) {
        var result = MAPPER.createObjectNode();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            var key = entry.getKey().toString();
            var value = valueToJsonValue(entry.getValue());
            addToObjectNode(result, key, value);
        }
        return result;
    }

    private JsonNode tupleToJsonNode(Tuple<?> value) {
        var result = MAPPER.createArrayNode();
        for (Object element : value.elements()) {
            addToArrayNode(result, valueToJsonValue(element));
        }
        return result;
    }

    private Object valueToJsonValue(Object value) {
        if (value instanceof List<?>) return toJsonNode(value);
        if (value instanceof Map<?, ?>) return toJsonNode(value);
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
        throw new KSMLDataException("Can not add value to ObjectNode: " + value.getClass().getSimpleName());
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
        throw new KSMLDataException("Can not add value to ObjectNode: " + value.getClass().getSimpleName());
    }

    @Override
    public Object fromJsonNode(JsonNode node) {
        if (node.isArray()) return arrayToList((ArrayNode) node);
        if (node.isObject()) return objectToMap((ObjectNode) node);
        throw new KSMLDataException("Can not convert from JsonNode: " + node.toPrettyString());
    }

    private Object arrayToList(ArrayNode node) {
        var result = new ArrayList<>();
        for (var it = node.elements(); it.hasNext(); ) {
            var element = it.next();
            result.add(jsonValueToValue(element));
        }
        return result;
    }

    private Object objectToMap(ObjectNode node) {
        var result = new HashMap<String, Object>();
        for (var it = node.fields(); it.hasNext(); ) {
            var entry = it.next();
            result.put(entry.getKey(), jsonValueToValue(entry.getValue()));
        }
        return result;
    }

    private Object jsonValueToValue(JsonNode value) {
        try {
            if (value.isNull()) return null;
            if (value.isShort()) return value.shortValue();
            if (value.isInt()) return value.intValue();
            if (value.isLong()) return value.longValue();
            if (value.isDouble()) return value.doubleValue();
            if (value.isFloat()) return value.floatValue();
            if (value.isBinary()) return value.binaryValue();
            if (value.isTextual()) return value.textValue();
            if (value.isArray()) return fromJsonNode(value);
            if (value.isObject()) return fromJsonNode(value);
        } catch (Exception e) {
            throw new KSMLDataException("Can not convert from JSON value: " + value.getClass().getSimpleName(), e);
        }
        throw new KSMLDataException("Can not convert from JSON value: " + value.getClass().getSimpleName());
    }
}
