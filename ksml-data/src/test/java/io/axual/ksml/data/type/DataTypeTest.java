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

import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataNull;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DataTypeTest {

    @Test
    @DisplayName("UNKNOWN has Object.class, name 'Unknown', spec '?' and is assignable from anything")
    void unknownConstantBehavior() {
        var unknown = DataType.UNKNOWN;
        assertThat(unknown)
                .returns(Object.class, DataType::containerClass)
                .returns("Unknown", DataType::name)
                .returns("?", DataType::spec)
                .hasToString("?");

        // Assignable from other DataTypes
        assertThat(unknown.isAssignableFrom(new SimpleType(Integer.class, "integer")).isOK()).isTrue();
        assertThat(unknown.isAssignableFrom(new SimpleType(String.class, "string")).isOK()).isTrue();

        // Assignable from any Class
        assertThat(unknown.isAssignableFrom(Integer.class).isOK()).isTrue();
        assertThat(unknown.isAssignableFrom(String.class).isOK()).isTrue();

        // Assignable from any Object including null
        assertThat(unknown.isAssignableFrom(new Object()).isOK()).isTrue();
        assertThat(unknown.isAssignableFrom("abc").isOK()).isTrue();
        assertThat(unknown.isAssignableFrom((Object) null).isOK()).isTrue();
    }

    @Test
    @DisplayName("Default isAssignableFrom(DataObject) accepts DataNull and delegates to type for others")
    void defaultIsAssignableFromDataObject() {
        var numberType = new SimpleType(Number.class, "number");
        var stringType = new SimpleType(String.class, "string");

        // DataNull short-circuit should return true for any DataType
        assertThat(numberType.isAssignableFrom(DataNull.INSTANCE).isOK()).isTrue();
        assertThat(stringType.isAssignableFrom(DataNull.INSTANCE).isOK()).isTrue();

        // Delegates to isAssignableFrom(DataType)
        var dataInt = new DataInteger(42);
        assertThat(numberType.isAssignableFrom(dataInt).isOK()).isTrue();
        assertThat(stringType.isAssignableFrom(dataInt).isOK()).isFalse();
    }

    @Test
    @DisplayName("Default isAssignableFrom(Object) allows null and checks runtime class against DataType")
    void defaultIsAssignableFromObject() {
        var numberType = new SimpleType(Number.class, "number");
        var softly = new SoftAssertions();
        softly.assertThat(numberType.isAssignableFrom((Object) null).isOK()).isTrue();
        softly.assertThat(numberType.isAssignableFrom(Integer.valueOf(5)).isOK()).isTrue();
        softly.assertThat(numberType.isAssignableFrom("text").isOK()).isFalse();
        softly.assertAll();
    }
}
