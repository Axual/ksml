package io.axual.ksml.data.object;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library
 * %%
 * Copyright (C) 2021 - 2024 Axual B.V.
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

class DataIntegerTest {
    @Test
    @DisplayName("Constructors, type, equals/hashCode")
    void constructorsAndEquality() {
        var primitive15 = new DataInteger(15);
        var boxed15 = new DataInteger(Integer.valueOf(15));
        var boxed20 = new DataInteger(20);
        var nullDefaultConstruction = new DataInteger();
        var nullExplicitValue = new DataInteger(null);

        assertThat(primitive15)
                .returns(DataInteger.DATATYPE, DataInteger::type)
                .isEqualTo(boxed15)
                .isNotEqualTo(boxed20)
                .hasSameHashCodeAs(boxed15);

        assertThat(nullDefaultConstruction)
                .isEqualTo(nullExplicitValue)
                .hasSameHashCodeAs(nullExplicitValue)
                .returns(null, DataInteger::value);
    }

    @Test
    @DisplayName("toString across printers: numeric values unquoted; null shows schema prefix")
    void toStringPrinterModes() {
        var primitive = new DataInteger(325);
        assertThat(primitive.toString(INTERNAL)).isEqualTo("325");
        assertThat(primitive.toString(EXTERNAL_NO_SCHEMA)).isEqualTo("325");
        assertThat(primitive.toString(EXTERNAL_TOP_SCHEMA)).isEqualTo("325");
        assertThat(primitive.toString(EXTERNAL_ALL_SCHEMA)).isEqualTo("325");

        var nullDefaultConstruction = new DataInteger();
        assertThat(nullDefaultConstruction.toString(INTERNAL)).isEqualTo("integer: null");
        assertThat(nullDefaultConstruction.toString(EXTERNAL_NO_SCHEMA)).isEqualTo("integer: null");
        assertThat(nullDefaultConstruction.toString(EXTERNAL_TOP_SCHEMA)).isEqualTo("integer: null");
        assertThat(nullDefaultConstruction.toString(EXTERNAL_ALL_SCHEMA)).isEqualTo("integer: null");
    }
}
