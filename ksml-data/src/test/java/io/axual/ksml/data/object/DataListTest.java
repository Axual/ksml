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

import io.axual.ksml.data.type.DataType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.axual.ksml.data.object.DataObject.Printer.EXTERNAL_ALL_SCHEMA;
import static io.axual.ksml.data.object.DataObject.Printer.EXTERNAL_NO_SCHEMA;
import static io.axual.ksml.data.object.DataObject.Printer.EXTERNAL_TOP_SCHEMA;
import static io.axual.ksml.data.object.DataObject.Printer.INTERNAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DataListTest {

    @Test
    @DisplayName("Default constructor uses UNKNOWN value type; printers and emptiness")
    void defaultConstructorAndPrinters() {
        var list = new DataList();
        assertThat(list.isNull()).isFalse();
        assertThat(list.type().valueType()).isEqualTo(DataType.UNKNOWN);
        assertThat(list.size()).isZero();
        assertThat(list.toString(INTERNAL)).isEqualTo("[]");
        assertThat(list.toString(EXTERNAL_NO_SCHEMA)).isEqualTo("[]");
        assertThat(list.toString(EXTERNAL_TOP_SCHEMA)).isEqualTo("ListOfUnknown: []");
        assertThat(list.toString(EXTERNAL_ALL_SCHEMA)).isEqualTo("ListOfUnknown: []");
    }

    @Test
    @DisplayName("Explicit valueType enforces element type on add; addIfNotNull skips null")
    void addAndTypeVerification() {
        var list = new DataList(DataString.DATATYPE);
        list.add(new DataString("a"));
        list.addIfNotNull(null);
        assertThat(list.size()).isEqualTo(1);

        var dataInteger = new DataInteger(1);

        assertThatThrownBy(() -> list.add(dataInteger))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("Can not cast value of dataType");
    }

    @Test
    @DisplayName("Iterator, get, equals on non-null lists and different type inequality")
    void iterationAndEquals() {
        var a = new DataList(DataType.UNKNOWN);
        a.add(new DataInteger(1));
        a.add(new DataString("x"));

        var b = new DataList(DataType.UNKNOWN);
        b.add(new DataInteger(1));
        b.add(new DataString("x"));

        assertThat(a.size()).isEqualTo(2);
        assertThat(a.get(0)).isEqualTo(new DataInteger(1));
        assertThat(a.get(1)).isEqualTo(new DataString("x"));
        assertThat(a).isEqualTo(b);

        var c = new DataList(DataString.DATATYPE);
        c.add(new DataString("x"));
        assertThat(a).isNotEqualTo(c);
    }

    @Test
    @DisplayName("toString prints nested values with child printer; null-constructed list prints as empty []")
    void toStringNestedAndNullBehavior() {
        var list = new DataList(DataType.UNKNOWN);
        list.add(new DataInteger(1));
        list.add(new DataString("x"));
        assertThat(list.toString(INTERNAL)).isEqualTo("[1, \"x\"]");
        assertThat(list.toString(EXTERNAL_NO_SCHEMA)).isEqualTo("[1, \"x\"]");
        assertThat(list.toString(EXTERNAL_TOP_SCHEMA)).isEqualTo("ListOfUnknown: [1, \"x\"]");
        assertThat(list.toString(EXTERNAL_ALL_SCHEMA)).isEqualTo("ListOfUnknown: [1, \"x\"]");

        var nul = new DataList(DataString.DATATYPE, true);
        assertThat(nul.isNull()).isTrue();
        // Despite being null, DataList prints as empty list per current implementation
        assertThat(nul.toString(INTERNAL)).isEqualTo("[]");
        assertThat(nul.toString(EXTERNAL_TOP_SCHEMA)).isEqualTo("ListOfString: []");
    }
}
