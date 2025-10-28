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

public class KsmlFileOrDefinitionProvider implements CustomDefinitionProviderV2 {
    @Override
    public CustomDefinition provideCustomSchemaDefinition(final ResolvedType javaType, final SchemaGenerationContext context) {
        ObjectNode node;
        if (javaType.isInstanceOf(KsmlFilePath.class)) {
            node = context.createDefinition(context.getTypeContext().resolve(String.class));
        } else if (javaType.isInstanceOf(KsmlInlineDefinition.class)) {
            node = context.createDefinition(context.getTypeContext().resolve(ObjectNode.class));
        } else {
            return null;
        }
        return new CustomDefinition(node, CustomDefinition.DefinitionType.INLINE, CustomDefinition.AttributeInclusion.NO);
    }
}
