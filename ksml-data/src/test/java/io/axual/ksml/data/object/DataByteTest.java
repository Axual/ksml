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

class DataByteTest {

    @Test
    @DisplayName("Constructors, type, equals/hashCode")
    void constructorsAndEquality() {
        var primitive7 = new DataByte((byte) 7);
        var boxed7 = new DataByte(Byte.valueOf((byte) 7));
        var primitive8 = new DataByte((byte) 8);
        var nullDefaultConstruction = new DataByte();
        var nullExplicitValue = new DataByte(null);

        assertThat(primitive7)
                .returns(DataByte.DATATYPE, DataByte::type)
                .isEqualTo(boxed7)
                .isNotEqualTo(primitive8)
                .hasSameHashCodeAs(boxed7);

        assertThat(nullDefaultConstruction)
                .isEqualTo(nullExplicitValue)
                .hasSameHashCodeAs(nullExplicitValue)
                .returns(null, DataByte::value);
    }

    @Test
    @DisplayName("toString across printers: numeric values unquoted; null shows schema prefix")
    void toStringPrinterModes() {
        var primitive10 = new DataByte((byte) 10);
        assertThat(primitive10.toString(INTERNAL)).isEqualTo("10");
        assertThat(primitive10.toString(EXTERNAL_NO_SCHEMA)).isEqualTo("10");
        assertThat(primitive10.toString(EXTERNAL_TOP_SCHEMA)).isEqualTo("10");
        assertThat(primitive10.toString(EXTERNAL_ALL_SCHEMA)).isEqualTo("10");

        var nullDefaultConstruction = new DataByte();
        assertThat(nullDefaultConstruction.toString(INTERNAL)).isEqualTo("byte: null");
        assertThat(nullDefaultConstruction.toString(EXTERNAL_NO_SCHEMA)).isEqualTo("byte: null");
        assertThat(nullDefaultConstruction.toString(EXTERNAL_TOP_SCHEMA)).isEqualTo("byte: null");
        assertThat(nullDefaultConstruction.toString(EXTERNAL_ALL_SCHEMA)).isEqualTo("byte: null");
    }
}
