package io.axual.ksml.testutil;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2024 Axual B.V.
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class JsonVerifier {

    private final JsonNode rootNode;

    private JsonNode cursor;

    private JsonVerifier(String json) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        this.rootNode = mapper.readTree(json);
        this.cursor = rootNode;
    }

    public static JsonVerifier verifyJson(String json) throws JsonProcessingException {
        return new JsonVerifier(json);
    }

    /**
     * Drill down to the named node, starting from the root node.
     *
     * @param name
     * @return
     */
    public JsonVerifier hasNode(String name) {
        JsonNode child = rootNode.get(name);
        if (child == null) {
            fail(String.format("No node named %s was found in node %s", name, rootNode));
        }
        cursor = child;
        return this;
    }

    /**
     * Drill don to the named node. starting from the node we're currently at.
     *
     * @param name
     * @return
     */
    public JsonVerifier withChild(String name) {
        JsonNode child = cursor.get(name);
        if (child == null) {
            fail(String.format("No node named %s was found in node %s", name, rootNode));
        }
        cursor = child;
        return this;
    }

    public JsonVerifier withTextValue(String expectedText) {
        assertEquals(expectedText, cursor.textValue(), String.format("Node value does not match in %s", cursor));
        return this;
    }
}
