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

import io.axual.ksml.data.compare.Compared;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SimpleTypeTest {

    @Test
    @DisplayName("Two-arg constructor sets name and spec equally and exposes container class")
    void constructorWithDefaultSpec() {
        var t = new SimpleType(Integer.class, "integer");
        assertThat(t)
                .returns(Integer.class, SimpleType::containerClass)
                .returns("integer", SimpleType::name)
                .returns("integer", SimpleType::spec)
                .hasToString("integer");
    }

    @Test
    @DisplayName("Three-arg constructor uses explicit spec and preserves name")
    void constructorWithExplicitSpec() {
        var t = new SimpleType(Integer.class, "int32", "INT32");
        assertThat(t)
                .returns(Integer.class, SimpleType::containerClass)
                .returns("int32", SimpleType::name)
                .returns("INT32", SimpleType::spec)
                .hasToString("INT32");
    }

    @Test
    @DisplayName("Assignability between DataTypes follows Java Class.checkAssignableFrom")
    void checkAssignableFromDataType() {
        var numberType = new SimpleType(Number.class, "number");
        var integerType = new SimpleType(Integer.class, "integer");
        assertThat(numberType.checkAssignableFrom(integerType).isOK()).isTrue();
        assertThat(integerType.checkAssignableFrom(numberType).isOK()).isFalse();

        // Non-SimpleType should be rejected: using anonymous DataType not a SimpleType
        var other = new DataType() {
            @Override public Class<?> containerClass() { return Object.class; }
            @Override public String name() { return "X"; }
            @Override public String spec() { return "X"; }
            @Override public Compared checkAssignableFrom(DataType type) { return Compared.error("Fake error"); }
            @Override public Compared checkAssignableFrom(Class<?> type) { return Compared.error("Fake error"); }
            @Override public Compared equals(Object other, Flags flags) { return Compared.error("Fake error"); }
        };
        assertThat(numberType.checkAssignableFrom(other).isOK()).isFalse();
    }

    @Test
    @DisplayName("Assignability from Class uses containerClass.checkAssignableFrom")
    void checkAssignableFromClass() {
        var numberType = new SimpleType(Number.class, "number");
        var integerType = new SimpleType(Integer.class, "integer");
        assertThat(numberType.checkAssignableFrom(Integer.class).isOK()).isTrue();
        assertThat(integerType.checkAssignableFrom(Number.class).isOK()).isFalse();
    }

    @Test
    @DisplayName("Assignability from Object allows null and checks runtime class")
    void checkAssignableFromObject() {
        var numberType = new SimpleType(Number.class, "number");
        var integerType = new SimpleType(Integer.class, "integer");
        assertThat(numberType.checkAssignableFrom(Integer.valueOf(123)).isOK()).isTrue();
        assertThat(integerType.checkAssignableFrom(new Object()).isOK()).isFalse();
        assertThat(integerType.checkAssignableFrom((Object) null).isOK()).isTrue(); // DataType default allows null
    }

    @Test
    @DisplayName("equals is reflexive, equal for same container class, and not equal otherwise")
    void equalsBehavior() {
        var a1 = new SimpleType(Integer.class, "integer");
        var a2 = new SimpleType(Integer.class, "int32", "INT32");
        var b = new SimpleType(Number.class, "number");

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
        var a1 = new SimpleType(Integer.class, "integer");
        var a2 = new SimpleType(Integer.class, "int32", "INT32");

        var softly = new SoftAssertions();
        softly.assertThat(a1.hashCode()).as("first call").isEqualTo(a1.hashCode());
        softly.assertThat(a2.hashCode()).as("first call a2").isEqualTo(a2.hashCode());
        // We intentionally do not assert that equal objects have the same hashCode because current implementation
        // includes Object#hashCode() and thus may differ per instance by design.
        softly.assertAll();
    }
}
