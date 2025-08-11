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

import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.assertj.core.api.Assertions.assertThat;

class ListTypeTest {

    @Test
    @DisplayName("Default constructor uses UNKNOWN subtype and builds name/spec accordingly")
    void defaultConstructor() {
        ListType t = new ListType();
        assertThat(t)
                .returns(java.util.List.class, ListType::containerClass)
                .returns("ListOfUnknown", ListType::name)
                .returns("[?]", ListType::spec)
                .hasToString("ListOfUnknown");
        assertThat(t.subTypeCount()).isEqualTo(1);
        assertThat(t.valueType()).isEqualTo(DataType.UNKNOWN);
    }

    @Test
    @DisplayName("Explicit value type determines name/spec and valueType() accessor")
    void explicitValueType() {
        SimpleType intType = new SimpleType(Integer.class, "integer");
        ListType t = new ListType(intType);
        assertThat(t)
                .returns("ListOfInteger", ListType::name)
                .returns("[integer]", ListType::spec);
        assertThat(t.valueType()).isEqualTo(intType);
    }

    @Test
    @DisplayName("Assignability compares element types via ComplexType logic")
    void assignabilityBetweenLists() {
        ListType numberList = new ListType(new SimpleType(Number.class, "number"));
        ListType integerList = new ListType(new SimpleType(Integer.class, "integer"));
        assertThat(numberList.isAssignableFrom(integerList)).isTrue();
        assertThat(integerList.isAssignableFrom(numberList)).isFalse();

        // Different container class (e.g., MapType) is not assignable
        MapType mapOfNumber = new MapType(new SimpleType(Number.class, "number"));
        assertThat(numberList.isAssignableFrom(mapOfNumber)).isFalse();
    }

    @Test
    @DisplayName("Assignability from Class uses containerClass.isAssignableFrom(Class)")
    void assignabilityFromClass() {
        ListType anyList = new ListType();
        assertThat(anyList.isAssignableFrom(ArrayList.class)).isTrue();
        assertThat(anyList.isAssignableFrom(String.class)).isFalse();
    }

    @Test
    @DisplayName("createFrom extracts value type when source is a compatible ComplexType; otherwise returns null")
    void createFromBehavior() {
        SimpleType intType = new SimpleType(Integer.class, "integer");
        ListType source = new ListType(intType);
        ListType created = ListType.createFrom(source);
        assertThat(created).isNotNull();
        assertThat(created.valueType()).isEqualTo(intType);

        // Not a list-like complex type
        MapType mapType = new MapType(intType);
        assertThat(ListType.createFrom(mapType)).isNull();
        // Not a complex type
        assertThat(ListType.createFrom(intType)).isNull();
    }

    @Test
    @DisplayName("equals checks mutual assignability; hashCode is consistent per instance")
    void equalsAndHashCode() {
        ListType a1 = new ListType(DataType.UNKNOWN);
        ListType a2 = new ListType(DataType.UNKNOWN);
        ListType b = new ListType(new SimpleType(Integer.class, "integer"));

        SoftAssertions softly = new SoftAssertions();
        // Reflexivity
        softly.assertThat(a1.equals(a1)).isTrue();
        // Same subtype Unknown -> likely equal (mutual assignable)
        softly.assertThat(a1).isEqualTo(a2);
        // Different subtypes not mutually assignable -> not equal
        softly.assertThat(a1).isNotEqualTo(b);
        // hashCode consistent per instance
        softly.assertThat(a1.hashCode()).isEqualTo(a1.hashCode());
        softly.assertThat(b.hashCode()).isEqualTo(b.hashCode());
        softly.assertAll();
    }
}
