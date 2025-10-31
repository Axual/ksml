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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.victools.jsonschema.generator.CustomDefinition;
import com.github.victools.jsonschema.generator.CustomDefinitionProviderV2;
import com.github.victools.jsonschema.generator.FieldScope;
import com.github.victools.jsonschema.generator.SchemaGenerationContext;

import java.util.function.BiFunction;

/**
 * Custom JSON Schema definition provider and Additional Properties Resolver for the {@link StringMap} type.
 *
 * This type adds the string, integer, number and boolean type as possible values. Jackson turns these all into Strings internally.
 *
 * It adds the same additional properties to both field and definition, which results in the field definition to be cleaned.
 */
public class StringMapDefinitionPropertiesResolver implements CustomDefinitionProviderV2, BiFunction<FieldScope, SchemaGenerationContext, JsonNode> {
    @Override
    public JsonNode apply(final FieldScope fieldScope, final SchemaGenerationContext context) {
        var resolvedType = fieldScope.getType();
        if (resolvedType.isInstanceOf(StringMap.class)) {
            var additionalPropSchema = context.getGeneratorConfig().createObjectNode();
            addTypes(additionalPropSchema);
            return additionalPropSchema;
        }
        return null;
    }

    @Override
    public CustomDefinition provideCustomSchemaDefinition(final ResolvedType resolvedType, final SchemaGenerationContext context) {
        if (resolvedType.isInstanceOf(StringMap.class)) {
            // Create schema that accepts string, number, integer, boolean
            ObjectNode schema = context.createStandardDefinition(resolvedType, this);
            schema.put("type", "object");

            ObjectNode additionalPropSchema = schema.putObject("additionalProperties");
            addTypes(additionalPropSchema);

            return new CustomDefinition(schema, CustomDefinition.DefinitionType.STANDARD, CustomDefinition.AttributeInclusion.NO);
        }
        return null;
    }

    private void addTypes(ObjectNode additionalPropSchema) {
        var types = additionalPropSchema.putArray("type");
        types.add("string");
        types.add("number");
        types.add("integer");
        types.add("boolean");
    }
}
