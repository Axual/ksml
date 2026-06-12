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

import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonParser;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.deser.std.StdDeserializer;
import tools.jackson.databind.node.ObjectNode;

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
    public KsmlFileOrDefinition deserialize(JsonParser p, DeserializationContext context) throws JacksonException {
        // Read the current JSON token tree to inspect its kind (string vs object)
        JsonNode node = context.readTree(p);

        if (node.isString()) {
            // String -> file path variant
            return new KsmlFilePath(node.asString());
        } else if (node instanceof ObjectNode objectNode) {
            // Object -> inline JSON definition variant
            return new KsmlInlineDefinition(objectNode);
        }

        // Any other JSON kind is invalid for this union type
        throw context.instantiationException(KsmlFileOrDefinition.class, "Expected String or Object, got: " + node.getNodeType());
    }
}
