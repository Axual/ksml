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

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import io.axual.ksml.data.schema.StructSchema;

import static io.axual.ksml.data.object.DataObject.Printer.EXTERNAL_ALL_SCHEMA;
import static io.axual.ksml.data.object.DataObject.Printer.EXTERNAL_NO_SCHEMA;
import static io.axual.ksml.data.object.DataObject.Printer.EXTERNAL_TOP_SCHEMA;
import static io.axual.ksml.data.object.DataObject.Printer.INTERNAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DataStructTest {

    @Test
    @DisplayName("Default constructor, printers and emptiness")
    void defaultConstructorAndPrinters() {
        var st = new DataStruct();
        assertThat(st.isNull()).isFalse();
        assertThat(st.size()).isZero();
        assertThat(st.toString(INTERNAL)).isEqualTo("{}");
        assertThat(st.toString(EXTERNAL_NO_SCHEMA)).isEqualTo("{}");
        assertThat(st.toString(EXTERNAL_TOP_SCHEMA)).isEqualTo("Struct: {}");
        assertThat(st.toString(EXTERNAL_ALL_SCHEMA)).isEqualTo("Struct: {}");
    }

    @Test
    @DisplayName("Null struct prints 'null' with schema prefix in TOP/ALL (using schema name) and throws on put")
    void nullStructBehavior() {
        var schema = new StructSchema("io.axual.ksml", "User", null, List.of());
        var nul = new DataStruct(schema, true);
        assertThat(nul.isNull()).isTrue();
        assertThat(nul.toString(INTERNAL)).isEqualTo("null");
        assertThat(nul.toString(EXTERNAL_NO_SCHEMA)).isEqualTo("null");
        assertThat(nul.toString(EXTERNAL_TOP_SCHEMA)).isEqualTo("User: null");
        assertThat(nul.toString(EXTERNAL_ALL_SCHEMA)).isEqualTo("User: null");

        assertThatThrownBy(() -> nul.put("a", new DataString("x")))
                .isInstanceOf(io.axual.ksml.data.exception.DataException.class)
                .hasMessageContaining("Can not add item to a NULL Struct");
        assertThatThrownBy(() -> nul.putIfAbsent("a", new DataString("x")))
                .isInstanceOf(io.axual.ksml.data.exception.DataException.class)
                .hasMessageContaining("Can not add item to a NULL Struct");
    }

    @Test
    @DisplayName("Comparator orders regular keys before meta '@' keys and lexicographically within groups")
    void comparatorOrdering() {
        var st = new DataStruct();
        st.put("b", new DataInteger(2));
        st.put("@meta", new DataString("m"));
        st.put("a", new DataString("x"));
        assertThat(st.toString(INTERNAL)).isEqualTo("{\"a\": \"x\", \"b\": 2, \"@meta\": \"m\"}");
    }

    @Test
    @DisplayName("getIfPresent applies for matching class; getAs and getAsString behaviors")
    void gettersAndAppliers() {
        var st = new DataStruct();
        st.put("num", new DataInteger(5));
        st.put("str", new DataString("hello"));

        var applied = new AtomicInteger(0);
        st.getIfPresent("num", DataInteger.class, v -> applied.incrementAndGet());
        st.getIfPresent("num", DataString.class, v -> applied.addAndGet(100));
        assertThat(applied.get()).isEqualTo(1);

        // getAs when type matches
        assertThat(st.getAs("str", DataString.class)).isEqualTo(new DataString("hello"));
        // getAs with default when type mismatches
        assertThat(st.getAs("str", DataInteger.class, new DataInteger(9))).isEqualTo(new DataInteger(9));

        // getAsString wraps non-string value using its toString()
        assertThat(st.getAsString("num")).isEqualTo(new DataString("5"));
    }

    @Test
    @DisplayName("equals on non-null structs with same content and default type; inequality on different content")
    void equalsSemantics() {
        var a = new DataStruct();
        a.put("k1", new DataInteger(1));
        a.put("k2", new DataString("x"));

        var b = new DataStruct();
        b.put("k1", new DataInteger(1));
        b.put("k2", new DataString("x"));

        var c = new DataStruct();
        c.put("k1", new DataInteger(2));
        c.put("k2", new DataString("x"));

        assertThat(a)
                .isEqualTo(b)
                .isNotEqualTo(c);
    }
}
