package io.axual.ksml.data.type;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library
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

import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.schema.EnumSchema;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class EnumTypeTest {

    @Test
    @DisplayName("Constructor sets container class to String and name/spec to 'enum'; symbols are retained")
    void constructorAndProperties() {
        var type = new EnumType(new EnumSchema(List.of(new EnumSchema.Symbol("A"), new EnumSchema.Symbol("B", "desc", 1))));
        assertThat(type)
                .returns(String.class, EnumType::containerClass)
                .returns("enum", EnumType::name)
                .returns("enum", EnumType::spec)
                .hasToString("enum");
        assertThat(type.schema().symbols()).extracting(EnumSchema.Symbol::name).containsExactly("A", "B");
    }

    @Test
    @DisplayName("isAssignableFrom(DataObject) accepts only DataString values matching one of the symbols; null is rejected")
    void isAssignableFromDataObject() {
        var type = new EnumType(new EnumSchema(List.of(new EnumSchema.Symbol("A"), new EnumSchema.Symbol("B"))));

        // Matching symbols
        assertThat(type.isAssignableFrom(new DataString("A")).isOK()).isTrue();
        assertThat(type.isAssignableFrom(new DataString("B")).isOK()).isTrue();

        // Non-matching string
        assertThat(type.isAssignableFrom(new DataString("C")).isOK()).isFalse();

        // Null is not considered assignable for EnumType
        assertThat(type.isAssignableFrom(DataNull.INSTANCE).isOK()).isFalse();
    }

    @Test
    @DisplayName("isAssignableFrom(DataObject) uses DataObject.toString for comparison (no quotes for internal printing)")
    void comparisonUsesToStringValue() {
        var type = new EnumType(new EnumSchema(List.of(new EnumSchema.Symbol("hello"))));
        var ds = new DataString("hello");
        var softly = new SoftAssertions();
        softly.assertThat(ds.toString()).isEqualTo("hello");
        softly.assertThat(type.isAssignableFrom(ds).isOK()).isTrue();
        softly.assertAll();
    }
}
