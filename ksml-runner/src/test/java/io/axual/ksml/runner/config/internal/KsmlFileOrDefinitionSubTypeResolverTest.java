package io.axual.ksml.runner.config.internal;

/*-
 * ========================LICENSE_START=================================
 * KSML Runner
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

import com.fasterxml.classmate.ResolvedType;
import com.github.victools.jsonschema.generator.SchemaGenerationContext;
import com.github.victools.jsonschema.generator.TypeContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link KsmlFileOrDefinitionSubTypeResolver}, verifying that the two concrete subtypes are
 * advertised for the {@link KsmlFileOrDefinition} union and that other types are left untouched.
 */
@ExtendWith(MockitoExtension.class)
class KsmlFileOrDefinitionSubTypeResolverTest {

    private final KsmlFileOrDefinitionSubTypeResolver resolver = new KsmlFileOrDefinitionSubTypeResolver();

    @Mock
    private ResolvedType declaredType;
    @Mock
    private TypeContext typeContext;
    @Mock
    private SchemaGenerationContext context;
    @Mock
    private ResolvedType filePathType;
    @Mock
    private ResolvedType inlineType;

    @Test
    @DisplayName("The union type resolves to both concrete subtypes")
    void resolvesBothSubtypesForUnion() {
        // doReturn avoids the unchecked-cast warning that when(...).thenReturn(...) triggers on Class<?>.
        doReturn(KsmlFileOrDefinition.class).when(declaredType).getErasedType();

        when(context.getTypeContext()).thenReturn(typeContext);

        when(typeContext.resolveSubtype(declaredType, KsmlFilePath.class)).thenReturn(filePathType);
        when(typeContext.resolveSubtype(declaredType, KsmlInlineDefinition.class)).thenReturn(inlineType);

        final var subtypes = resolver.findSubtypes(declaredType, context);

        assertThat(subtypes).containsExactly(filePathType, inlineType);
    }

    @Test
    @DisplayName("Other declared types are not given any subtypes")
    void returnsNullForUnrelatedType() {
        doReturn(String.class).when(declaredType).getErasedType();

        // Returning null signals "no special subtypes" to the schema generator.
        assertThat(resolver.findSubtypes(declaredType, context)).isNull();
    }
}
