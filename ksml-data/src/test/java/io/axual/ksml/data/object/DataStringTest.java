package io.axual.ksml.data.object;

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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.axual.ksml.data.object.DataObject.Printer.EXTERNAL_ALL_SCHEMA;
import static io.axual.ksml.data.object.DataObject.Printer.EXTERNAL_NO_SCHEMA;
import static io.axual.ksml.data.object.DataObject.Printer.EXTERNAL_TOP_SCHEMA;
import static io.axual.ksml.data.object.DataObject.Printer.INTERNAL;
import static org.assertj.core.api.Assertions.assertThat;

class DataStringTest {

    @Test
    @DisplayName("Constructors set value and type; equals/hashCode follow value semantics")
    void constructorsAndEquality() {
        var abcFirst = new DataString("abc");
        var abcSecond = new DataString(new String("abc"));
        var def = new DataString("def");
        var nullDefaultConstruction = new DataString();
        var nullExplicitValue = new DataString(null);

        assertThat(abcFirst)
                .returns(DataString.DATATYPE, DataString::type)
                .isEqualTo(abcSecond)
                .isNotEqualTo(def)
                .hasSameHashCodeAs(abcSecond);

        assertThat(nullDefaultConstruction)
                .isEqualTo(nullExplicitValue)
                .hasSameHashCodeAs(nullExplicitValue)
                .returns(null, DataString::value);
    }

    @Test
    @DisplayName("isEmpty is true for null or empty string; false otherwise")
    void isEmptyBehavior() {
        assertThat(new DataString().isEmpty()).isTrue();
        assertThat(new DataString("").isEmpty()).isTrue();
        assertThat(new DataString(" ").isEmpty()).isFalse();
        assertThat(new DataString("x").isEmpty()).isFalse();
    }

    @Test
    @DisplayName("from returns null when input is null; otherwise wraps value")
    void fromFactory() {
        assertThat(DataString.from(null)).isNull();
        assertThat(DataString.from("z")).isEqualTo(new DataString("z"));
    }

    @Test
    @DisplayName("toString across printers: INTERNAL unquoted; EXTERNAL quoted; null shows schema prefix")
    void toStringPrinterModes() {
        var value = new DataString("abc");
        assertThat(value.toString(INTERNAL)).isEqualTo("abc");
        assertThat(value.toString(EXTERNAL_NO_SCHEMA)).isEqualTo("\"abc\"");
        assertThat(value.toString(EXTERNAL_TOP_SCHEMA)).isEqualTo("\"abc\"");
        assertThat(value.toString(EXTERNAL_ALL_SCHEMA)).isEqualTo("\"abc\"");

        var nullDefaultConstruction = new DataString();
        assertThat(nullDefaultConstruction.toString(INTERNAL)).isEqualTo("string: null");
        assertThat(nullDefaultConstruction.toString(EXTERNAL_NO_SCHEMA)).isEqualTo("string: null");
        assertThat(nullDefaultConstruction.toString(EXTERNAL_TOP_SCHEMA)).isEqualTo("string: null");
        assertThat(nullDefaultConstruction.toString(EXTERNAL_ALL_SCHEMA)).isEqualTo("string: null");
    }
}
