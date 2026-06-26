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
import com.github.victools.jsonschema.generator.TypeContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link KsmlFileOrDefinitionSubTypeResolver}, verifying that the two concrete subtypes are
 * advertised for the {@link KsmlFileOrDefinition} union and that other types are left untouched.
 */
class KsmlFileOrDefinitionSubTypeResolverTest {

    private final KsmlFileOrDefinitionSubTypeResolver resolver = new KsmlFileOrDefinitionSubTypeResolver();

    @Test
    @DisplayName("The union type resolves to both concrete subtypes")
    void resolvesBothSubtypesForUnion() {
        final var declaredType = mock(ResolvedType.class);
        when(declaredType.getErasedType()).thenReturn((Class) KsmlFileOrDefinition.class);

        final var typeContext = mock(TypeContext.class);
        final var context = mock(SchemaGenerationContext.class);
        when(context.getTypeContext()).thenReturn(typeContext);

        final var filePathType = mock(ResolvedType.class);
        final var inlineType = mock(ResolvedType.class);
        when(typeContext.resolveSubtype(declaredType, KsmlFilePath.class)).thenReturn(filePathType);
        when(typeContext.resolveSubtype(declaredType, KsmlInlineDefinition.class)).thenReturn(inlineType);

        final var subtypes = resolver.findSubtypes(declaredType, context);

        assertThat(subtypes).containsExactly(filePathType, inlineType);
    }

    @Test
    @DisplayName("Other declared types are not given any subtypes")
    void returnsNullForUnrelatedType() {
        final var declaredType = mock(ResolvedType.class);
        when(declaredType.getErasedType()).thenReturn((Class) String.class);

        // Returning null signals "no special subtypes" to the schema generator.
        assertThat(resolver.findSubtypes(declaredType, mock(SchemaGenerationContext.class))).isNull();
    }
}
