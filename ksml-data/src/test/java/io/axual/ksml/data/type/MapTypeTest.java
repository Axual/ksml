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

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class MapTypeTest {

    @Test
    @DisplayName("Default constructor uses UNKNOWN value type, key type STRING, and builds name/spec accordingly")
    void defaultConstructor() {
        var t = new MapType();
        assertThat(t)
                .returns(Map.class, MapType::containerClass)
                .returns("MapOfUnknown", MapType::name)
                .returns("map(?)", MapType::spec)
                .hasToString("MapOfUnknown");
        assertThat(t.subTypeCount()).isEqualTo(2);
        assertThat(t.keyType()).isEqualTo(io.axual.ksml.data.object.DataString.DATATYPE);
        assertThat(t.valueType()).isEqualTo(DataType.UNKNOWN);
    }

    @Test
    @DisplayName("Explicit value type determines name/spec and valueType() accessor; keyType remains STRING")
    void explicitValueType() {
        var intType = new SimpleType(Integer.class, "integer");
        var t = new MapType(intType);
        assertThat(t)
                .returns("MapOfInteger", MapType::name)
                .returns("map(integer)", MapType::spec);
        assertThat(t.keyType()).isEqualTo(io.axual.ksml.data.object.DataString.DATATYPE);
        assertThat(t.valueType()).isEqualTo(intType);
    }

    @Test
    @DisplayName("Assignability compares value types via ComplexType logic")
    void assignabilityBetweenMaps() {
        var numberMap = new MapType(new SimpleType(Number.class, "number"));
        var integerMap = new MapType(new SimpleType(Integer.class, "integer"));
        assertThat(numberMap.isAssignableFrom(integerMap)).isTrue();
        assertThat(integerMap.isAssignableFrom(numberMap)).isFalse();

        // Different container class (e.g., ListType) is not assignable
        var listOfNumber = new ListType(new SimpleType(Number.class, "number"));
        assertThat(numberMap.isAssignableFrom(listOfNumber)).isFalse();
    }

    @Test
    @DisplayName("Assignability from Class uses containerClass.isAssignableFrom(Class)")
    void assignabilityFromClass() {
        var anyMap = new MapType();
        assertThat(anyMap.isAssignableFrom(HashMap.class)).isTrue();
        assertThat(anyMap.isAssignableFrom(String.class)).isFalse();
    }

    @Test
    @DisplayName("equals checks mutual assignability; hashCode is consistent per instance")
    void equalsAndHashCode() {
        var a1 = new MapType(DataType.UNKNOWN);
        var a2 = new MapType(DataType.UNKNOWN);
        var b = new MapType(new SimpleType(Integer.class, "integer"));

        var softly = new SoftAssertions();
        // Reflexivity
        softly.assertThat(a1.equals(a1)).isTrue();
        // Same subtype Unknown -> equal (mutual assignable)
        softly.assertThat(a1).isEqualTo(a2);
        // Different subtypes not mutually assignable -> not equal
        softly.assertThat(a1).isNotEqualTo(b);
        // hashCode consistent per instance
        softly.assertThat(a1.hashCode()).isEqualTo(a1.hashCode());
        softly.assertThat(b.hashCode()).isEqualTo(b.hashCode());
        softly.assertAll();
    }
}
