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

class DataBooleanTest {

    @Test
    @DisplayName("Constructors, type, equals/hashCode")
    void constructorsAndEquality() {
        var primitiveTrue = new DataBoolean(true);
        var boxedTrue = new DataBoolean(Boolean.TRUE);
        var primitiveFalse = new DataBoolean(false);
        var boxedFalse = new DataBoolean(Boolean.FALSE);
        var nullDefaultConstruction = new DataBoolean();
        var nullExplicitValue = new DataBoolean(null);

        assertThat(primitiveTrue)
                .returns(DataBoolean.DATATYPE, DataBoolean::type)
                .isEqualTo(boxedTrue)
                .isNotEqualTo(primitiveFalse)
                .isNotEqualTo(boxedFalse)
                .hasSameHashCodeAs(boxedTrue);

        assertThat(primitiveFalse)
                .returns(DataBoolean.DATATYPE, DataBoolean::type)
                .isEqualTo(boxedFalse)
                .isNotEqualTo(primitiveTrue)
                .isNotEqualTo(boxedTrue)
                .hasSameHashCodeAs(boxedFalse);

        assertThat(nullDefaultConstruction)
                .isEqualTo(nullExplicitValue)
                .hasSameHashCodeAs(nullExplicitValue)
                .returns(null, DataBoolean::value);
    }

    @Test
    @DisplayName("toString across printers: boolean values unquoted; null shows schema prefix")
    void toStringPrinterModes() {
        var primitiveTrue = new DataBoolean(true);
        assertThat(primitiveTrue.toString(INTERNAL)).isEqualTo("true");
        assertThat(primitiveTrue.toString(EXTERNAL_NO_SCHEMA)).isEqualTo("true");
        assertThat(primitiveTrue.toString(EXTERNAL_TOP_SCHEMA)).isEqualTo("true");
        assertThat(primitiveTrue.toString(EXTERNAL_ALL_SCHEMA)).isEqualTo("true");

        var nullDefaultConstruction = new DataBoolean();
        assertThat(nullDefaultConstruction.toString(INTERNAL)).isEqualTo("boolean: null");
        assertThat(nullDefaultConstruction.toString(EXTERNAL_NO_SCHEMA)).isEqualTo("boolean: null");
        assertThat(nullDefaultConstruction.toString(EXTERNAL_TOP_SCHEMA)).isEqualTo("boolean: null");
        assertThat(nullDefaultConstruction.toString(EXTERNAL_ALL_SCHEMA)).isEqualTo("boolean: null");
    }
}
