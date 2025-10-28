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

import com.fasterxml.classmate.ResolvedType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.victools.jsonschema.generator.CustomDefinition;
import com.github.victools.jsonschema.generator.CustomDefinitionProviderV2;
import com.github.victools.jsonschema.generator.SchemaGenerationContext;

/**
 * Custom JSON Schema definition provider for the polymorphic {@link KsmlFileOrDefinition} type.
 *
 * It maps the concrete implementations to their respective JSON value kinds in the generated schema:
 * - {@link KsmlFilePath} -> a JSON string
 * - {@link KsmlInlineDefinition} -> a JSON object
 *
 * Returning an INLINE definition ensures that the schema for these leaf types is embedded directly
 * where referenced, avoiding separate $defs entries.
 */
public class KsmlFileOrDefinitionProvider implements CustomDefinitionProviderV2 {
    @Override
    public CustomDefinition provideCustomSchemaDefinition(final ResolvedType javaType, final SchemaGenerationContext context) {
        ObjectNode node;
        // Generate the schema node based on which subtype is being requested
        if (javaType.isInstanceOf(KsmlFilePath.class)) {
            // File path is represented as a simple string in JSON
            node = context.createDefinition(context.getTypeContext().resolve(String.class));
        } else if (javaType.isInstanceOf(KsmlInlineDefinition.class)) {
            // Inline definition is represented as a generic JSON object
            node = context.createDefinition(context.getTypeContext().resolve(ObjectNode.class));
        } else {
            // Let the generator handle other types
            return null;
        }
        return new CustomDefinition(node, CustomDefinition.DefinitionType.INLINE, CustomDefinition.AttributeInclusion.NO);
    }
}
