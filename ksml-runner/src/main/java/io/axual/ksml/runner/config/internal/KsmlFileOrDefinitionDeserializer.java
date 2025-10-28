package io.axual.ksml.runner.config.internal;

/*-
 * ========================LICENSE_START=================================
 * KSML Runner
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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;

// Deserializer
public class KsmlFileOrDefinitionDeserializer extends StdDeserializer<KsmlFileOrDefinition> {
    public KsmlFileOrDefinitionDeserializer() {
        super(KsmlFileOrDefinition.class);
    }

    @Override
    public KsmlFileOrDefinition deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {

        JsonNode node = p.getCodec().readTree(p);

        if (node.isTextual()) {
            return new KsmlFilePath(node.asText());
        } else if (node instanceof ObjectNode objectNode) {
            return new KsmlInlineDefinition(objectNode);
        }

        throw ctxt.instantiationException(KsmlFileOrDefinition.class, "Expected String or Object, got: " + node.getNodeType());
    }
}
