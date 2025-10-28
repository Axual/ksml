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
import com.github.victools.jsonschema.generator.SchemaGenerationContext;
import com.github.victools.jsonschema.generator.SubtypeResolver;
import com.github.victools.jsonschema.generator.TypeContext;

import java.util.Arrays;
import java.util.List;

/**
 * VicTools JSON Schema subtype resolver for the sealed union {@link KsmlFileOrDefinition}.
 *
 * This informs the schema generator about the two concrete implementations so that it can
 * render a composed schema (oneOf) for the union type where applicable.
 */
public class KsmlFileOrDefinitionSubTypeResolver implements SubtypeResolver {

    @Override
    public List<ResolvedType> findSubtypes(final ResolvedType declaredType, final SchemaGenerationContext context) {
        // Only provide subtypes for the KsmlFileOrDefinition union
        if (declaredType.getErasedType() == KsmlFileOrDefinition.class) {
            TypeContext typeContext = context.getTypeContext();
            return Arrays.asList(
                    typeContext.resolveSubtype(declaredType, KsmlFilePath.class),
                    typeContext.resolveSubtype(declaredType, KsmlInlineDefinition.class)
            );
        }
        // Returning null signals: no special subtypes for other types
        return null;
    }
}
