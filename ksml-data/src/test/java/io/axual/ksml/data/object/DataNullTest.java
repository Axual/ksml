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

import io.axual.ksml.data.type.SimpleType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.axual.ksml.data.object.DataObject.Printer.EXTERNAL_ALL_SCHEMA;
import static io.axual.ksml.data.object.DataObject.Printer.EXTERNAL_NO_SCHEMA;
import static io.axual.ksml.data.object.DataObject.Printer.EXTERNAL_TOP_SCHEMA;
import static io.axual.ksml.data.object.DataObject.Printer.INTERNAL;
import static org.assertj.core.api.Assertions.assertThat;

class DataNullTest {

    @Test
    @DisplayName("Singleton instance has correct type and consistent toString across printers")
    void singletonAndToString() {
        var dn = DataNull.INSTANCE;
        assertThat(dn.type()).isEqualTo(DataNull.DATATYPE);
        assertThat(dn.toString(INTERNAL)).isEqualTo("null: null");
        assertThat(dn.toString(EXTERNAL_NO_SCHEMA)).isEqualTo("null: null");
        assertThat(dn.toString(EXTERNAL_TOP_SCHEMA)).isEqualTo("null: null");
        assertThat(dn.toString(EXTERNAL_ALL_SCHEMA)).isEqualTo("null: null");
    }

    @Test
    @DisplayName("DataNull datatype assignability: only itself as DataType; only null as value; and DataObject singleton")
    void datatypeAssignability() {
        var dt = DataNull.DATATYPE;
        assertThat(dt.isAssignableFrom(DataNull.DATATYPE).isAssignable()).isTrue();
        // Different SimpleType instance with the same container/name is NOT considered assignable per override
        assertThat(dt.isAssignableFrom(new SimpleType(io.axual.ksml.data.value.Null.class, "null")).isAssignable()).isFalse();

        assertThat(dt.isAssignableFrom((Object) null).isAssignable()).isTrue();
        assertThat(dt.isAssignableFrom("not null").isAssignable()).isFalse();

        assertThat(dt.isAssignableFrom(DataNull.INSTANCE).isAssignable()).isTrue();
        assertThat(dt.isAssignableFrom(new DataString()).isAssignable()).isFalse();
    }
}
