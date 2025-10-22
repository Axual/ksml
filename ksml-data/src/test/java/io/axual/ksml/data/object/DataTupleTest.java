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

import io.axual.ksml.data.type.TupleType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.axual.ksml.data.object.DataObject.Printer.EXTERNAL_ALL_SCHEMA;
import static io.axual.ksml.data.object.DataObject.Printer.EXTERNAL_NO_SCHEMA;
import static io.axual.ksml.data.object.DataObject.Printer.EXTERNAL_TOP_SCHEMA;
import static io.axual.ksml.data.object.DataObject.Printer.INTERNAL;
import static org.assertj.core.api.Assertions.assertThat;

class DataTupleTest {

    @Test
    @DisplayName("Type derives from element types; equals/hashCode follow elements and type")
    void typeAndEquality() {
        var t1 = new DataTuple(new DataInteger(1), new DataString("x"));
        var t2 = new DataTuple(new DataInteger(1), new DataString("x"));
        var t3 = new DataTuple(new DataInteger(2), new DataString("x"));

        assertThat(t1.type()).isInstanceOf(TupleType.class);
        assertThat(((TupleType) t1.type()).subTypeCount()).isEqualTo(2);
        assertThat(((TupleType) t1.type()).subType(0)).isEqualTo(DataInteger.DATATYPE);
        assertThat(((TupleType) t1.type()).subType(1)).isEqualTo(DataString.DATATYPE);

        assertThat(t1).isEqualTo(t2).isNotEqualTo(t3).hasSameHashCodeAs(t2).doesNotHaveSameHashCodeAs(t3);
    }

    @Test
    @DisplayName("toString prints elements comma-separated in parentheses; top-level shows schema for TOP/ALL")
    void toStringPrinterModes() {
        var t = new DataTuple(new DataInteger(1), new DataString("x"));
        assertThat(t.toString(INTERNAL)).isEqualTo("(1, \"x\")");
        assertThat(t.toString(EXTERNAL_NO_SCHEMA)).isEqualTo("(1, \"x\")");
        assertThat(t.toString(EXTERNAL_TOP_SCHEMA)).isEqualTo("TupleOfIntegerAndString: (1, \"x\")");
        assertThat(t.toString(EXTERNAL_ALL_SCHEMA)).isEqualTo("TupleOfIntegerAndString: (1, \"x\")");
    }
}
