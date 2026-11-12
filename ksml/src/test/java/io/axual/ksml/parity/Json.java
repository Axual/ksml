package io.axual.ksml.parity;

/*-
 * ========================LICENSE_START=================================
 * KSML
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

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

/**
 * Small JSON helpers for the plain Kafka Streams reference topologies.
 */
public final class Json {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private Json() {
    }

    public static ObjectNode obj(String text) {
        return text == null ? null : (ObjectNode) MAPPER.readTree(text);
    }

    public static ObjectNode obj() {
        return MAPPER.createObjectNode();
    }

    public static String str(JsonNode node) {
        return node == null ? null : MAPPER.writeValueAsString(node);
    }

    public static long num(String text, String field) {
        return obj(text).get(field).asLong();
    }

    public static String text(String text, String field) {
        final var node = obj(text).get(field);
        return node == null || node.isNull() ? null : node.asString();
    }
}
