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

/**
 * Jackson deserializer for {@link KsmlFileOrDefinition}.
 *
 * The value can either be:
 * - a JSON string: interpreted as a file path to a KSML definition (produces {@link KsmlFilePath})
 * - a JSON object: interpreted as an inline KSML definition (produces {@link KsmlInlineDefinition})
 */
public class KsmlFileOrDefinitionDeserializer extends StdDeserializer<KsmlFileOrDefinition> {
    public KsmlFileOrDefinitionDeserializer() {
        super(KsmlFileOrDefinition.class);
    }

    @Override
    public KsmlFileOrDefinition deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        // Read the current JSON token tree to inspect its kind (string vs object)
        JsonNode node = p.getCodec().readTree(p);

        if (node.isTextual()) {
            // String -> file path variant
            return new KsmlFilePath(node.asText());
        } else if (node instanceof ObjectNode objectNode) {
            // Object -> inline JSON definition variant
            return new KsmlInlineDefinition(objectNode);
        }

        // Any other JSON kind is invalid for this union type
        throw ctxt.instantiationException(KsmlFileOrDefinition.class, "Expected String or Object, got: " + node.getNodeType());
    }
}
