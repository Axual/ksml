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
        var t = new ListType();
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
        var intType = new SimpleType(Integer.class, "integer");
        var t = new ListType(intType);
        assertThat(t)
                .returns("ListOfInteger", ListType::name)
                .returns("[integer]", ListType::spec);
        assertThat(t.valueType()).isEqualTo(intType);
    }

    @Test
    @DisplayName("Assignability compares element types via ComplexType logic")
    void assignabilityBetweenLists() {
        var numberList = new ListType(new SimpleType(Number.class, "number"));
        var integerList = new ListType(new SimpleType(Integer.class, "integer"));
        assertThat(numberList.isAssignableFrom(integerList).isOK()).isTrue();
        assertThat(integerList.isAssignableFrom(numberList).isOK()).isFalse();

        // Different container class (e.g., MapType) is not assignable
        var mapOfNumber = new MapType(new SimpleType(Number.class, "number"));
        assertThat(numberList.isAssignableFrom(mapOfNumber).isOK()).isFalse();
    }

    @Test
    @DisplayName("Assignability from Class uses containerClass.isAssignableFrom(Class)")
    void assignabilityFromClass() {
        var anyList = new ListType();
        assertThat(anyList.isAssignableFrom(ArrayList.class).isOK()).isTrue();
        assertThat(anyList.isAssignableFrom(String.class).isOK()).isFalse();
    }

    @Test
    @DisplayName("createFrom extracts value type when source is a compatible ComplexType; otherwise returns null")
    void createFromBehavior() {
        var intType = new SimpleType(Integer.class, "integer");
        var source = new ListType(intType);
        var created = ListType.createFrom(source);
        assertThat(created).isNotNull();
        assertThat(created.valueType()).isEqualTo(intType);

        // Not a list-like complex type
        var mapType = new MapType(intType);
        assertThat(ListType.createFrom(mapType)).isNull();
        // Not a complex type
        assertThat(ListType.createFrom(intType)).isNull();
    }

    @Test
    @DisplayName("equals checks mutual assignability; hashCode is consistent per instance")
    void equalsAndHashCode() {
        var a1 = new ListType(DataType.UNKNOWN);
        var a2 = new ListType(DataType.UNKNOWN);
        var b = new ListType(new SimpleType(Integer.class, "integer"));

        var softly = new SoftAssertions();
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
