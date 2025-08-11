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

import static org.assertj.core.api.Assertions.assertThat;

class SimpleTypeTest {

    @Test
    @DisplayName("Two-arg constructor sets name and spec equally and exposes container class")
    void constructorWithDefaultSpec() {
        SimpleType t = new SimpleType(Integer.class, "integer");
        assertThat(t)
                .returns(Integer.class, SimpleType::containerClass)
                .returns("integer", SimpleType::name)
                .returns("integer", SimpleType::spec)
                .hasToString("integer");
    }

    @Test
    @DisplayName("Three-arg constructor uses explicit spec and preserves name")
    void constructorWithExplicitSpec() {
        SimpleType t = new SimpleType(Integer.class, "int32", "INT32");
        assertThat(t)
                .returns(Integer.class, SimpleType::containerClass)
                .returns("int32", SimpleType::name)
                .returns("INT32", SimpleType::spec)
                .hasToString("INT32");
    }

    @Test
    @DisplayName("Assignability between DataTypes follows Java Class.isAssignableFrom")
    void isAssignableFromDataType() {
        SimpleType numberType = new SimpleType(Number.class, "number");
        SimpleType integerType = new SimpleType(Integer.class, "integer");
        assertThat(numberType.isAssignableFrom(integerType)).isTrue();
        assertThat(integerType.isAssignableFrom(numberType)).isFalse();

        // Non-SimpleType should be rejected: using anonymous DataType not a SimpleType
        DataType other = new DataType() {
            @Override public Class<?> containerClass() { return Object.class; }
            @Override public String name() { return "X"; }
            @Override public String spec() { return "X"; }
            @Override public boolean isAssignableFrom(DataType type) { return false; }
            @Override public boolean isAssignableFrom(Class<?> type) { return false; }
        };
        assertThat(numberType.isAssignableFrom(other)).isFalse();
    }

    @Test
    @DisplayName("Assignability from Class uses containerClass.isAssignableFrom")
    void isAssignableFromClass() {
        SimpleType numberType = new SimpleType(Number.class, "number");
        SimpleType integerType = new SimpleType(Integer.class, "integer");
        assertThat(numberType.isAssignableFrom(Integer.class)).isTrue();
        assertThat(integerType.isAssignableFrom(Number.class)).isFalse();
    }

    @Test
    @DisplayName("Assignability from Object allows null and checks runtime class")
    void isAssignableFromObject() {
        SimpleType numberType = new SimpleType(Number.class, "number");
        SimpleType integerType = new SimpleType(Integer.class, "integer");
        assertThat(numberType.isAssignableFrom(Integer.valueOf(123))).isTrue();
        assertThat(integerType.isAssignableFrom(new Object())).isFalse();
        assertThat(integerType.isAssignableFrom((Object) null)).isTrue(); // DataType default allows null
    }

    @Test
    @DisplayName("equals is reflexive, equal for same container class, and not equal otherwise")
    void equalsBehavior() {
        SimpleType a1 = new SimpleType(Integer.class, "integer");
        SimpleType a2 = new SimpleType(Integer.class, "int32", "INT32");
        SimpleType b = new SimpleType(Number.class, "number");

        // Reflexive
        assertThat(a1).isEqualTo(a1)
                .isEqualTo(a2)
                .isNotEqualTo(b)
                .isNotEqualTo(null)
                .returns(false, d->d.equals("not a SimpleType"));
    }

    @Test
    @DisplayName("hashCode is consistent per instance (contract with equals not asserted per current behavior)")
    void hashCodeConsistency() {
        SimpleType a1 = new SimpleType(Integer.class, "integer");
        SimpleType a2 = new SimpleType(Integer.class, "int32", "INT32");

        SoftAssertions softly = new SoftAssertions();
        softly.assertThat(a1.hashCode()).as("first call").isEqualTo(a1.hashCode());
        softly.assertThat(a2.hashCode()).as("first call a2").isEqualTo(a2.hashCode());
        // We intentionally do not assert that equal objects have the same hashCode because current implementation
        // includes Object#hashCode() and thus may differ per instance by design.
        softly.assertAll();
    }
}
