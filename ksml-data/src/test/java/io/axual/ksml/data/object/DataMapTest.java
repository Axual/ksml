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

import java.util.ArrayList;
import java.util.List;

import io.axual.ksml.data.type.DataType;

import static io.axual.ksml.data.object.DataObject.Printer.EXTERNAL_ALL_SCHEMA;
import static io.axual.ksml.data.object.DataObject.Printer.EXTERNAL_NO_SCHEMA;
import static io.axual.ksml.data.object.DataObject.Printer.EXTERNAL_TOP_SCHEMA;
import static io.axual.ksml.data.object.DataObject.Printer.INTERNAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DataMapTest {

    @Test
    @DisplayName("Default constructor uses UNKNOWN value type; printers and emptiness")
    void defaultConstructorAndPrinters() {
        var map = new DataMap();
        assertThat(map.isNull()).isFalse();
        assertThat(map.type().valueType()).isEqualTo(DataType.UNKNOWN);
        assertThat(map.size()).isZero();
        assertThat(map.toString(INTERNAL)).isEqualTo("{}");
        assertThat(map.toString(EXTERNAL_NO_SCHEMA)).isEqualTo("{}");
        assertThat(map.toString(EXTERNAL_TOP_SCHEMA)).isEqualTo("MapOfUnknown: {}");
        assertThat(map.toString(EXTERNAL_ALL_SCHEMA)).isEqualTo("MapOfUnknown: {}");
    }

    @Test
    @DisplayName("Null map prints 'null' with schema prefix in TOP/ALL and throws on put")
    void nullMapBehavior() {
        var nul = new DataMap(DataString.DATATYPE, true);
        assertThat(nul.isNull()).isTrue();
        assertThat(nul.toString(INTERNAL)).isEqualTo("null");
        assertThat(nul.toString(EXTERNAL_NO_SCHEMA)).isEqualTo("null");
        assertThat(nul.toString(EXTERNAL_TOP_SCHEMA)).isEqualTo("MapOfString: null");
        assertThat(nul.toString(EXTERNAL_ALL_SCHEMA)).isEqualTo("MapOfString: null");

        assertThatThrownBy(() -> nul.put("a", new DataString("x")))
                .isInstanceOf(io.axual.ksml.data.exception.DataException.class)
                .hasMessageContaining("Can not add item to a NULL Map");
        assertThatThrownBy(() -> nul.putIfAbsent("a", new DataString("x")))
                .isInstanceOf(io.axual.ksml.data.exception.DataException.class)
                .hasMessageContaining("Can not add item to a NULL Map");
    }

    @Test
    @DisplayName("Type enforcement on put and putIfAbsent; containsKey/get/forEach/entrySet; equals semantics")
    void operationsAndEquals() {
        var map = new DataMap(DataString.DATATYPE);
        map.put("a", new DataString("x"));
        // wrong type rejected
        var dataInteger = new DataInteger(1);
        assertThatThrownBy(() -> map.put("b", dataInteger))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("Can not cast value of dataType");

        // putIfAbsent
        var result = map.putIfAbsent("a", new DataString("y"));
        assertThat(result).isEqualTo(new DataString("x"));
        map.putIfNotNull("b", new DataString("z"));
        map.putIfNotNull("c", null);

        assertThat(map.containsKey("a")).isTrue();
        assertThat(map.get("a")).isEqualTo(new DataString("x"));

        List<String> visited = new ArrayList<>();
        map.forEach((k, v) -> visited.add(k + "=" + v.toString(INTERNAL)));
        assertThat(visited).containsExactly("a=x", "b=z"); // TreeMap orders by key

        // equals with same content and type
        var same = new DataMap(DataString.DATATYPE);
        same.put("a", new DataString("x"));
        same.put("b", new DataString("z"));
        assertThat(map).isEqualTo(same);

        // different value type makes equals false due to mutual assignability rule
        var differentType = new DataMap(DataType.UNKNOWN);
        differentType.put("a", new DataString("x"));
        differentType.put("b", new DataString("z"));
        assertThat(map).isNotEqualTo(differentType);
    }

    @Test
    @DisplayName("toString prints JSON-like with quoted keys and child printer for values")
    void toStringFormatting() {
        var map = new DataMap(DataType.UNKNOWN);
        map.put("b", new DataInteger(2));
        map.put("a", new DataString("x"));
        // Keys are sorted alphabetically: a first, then b
        assertThat(map.toString(INTERNAL)).isEqualTo("{\"a\": \"x\", \"b\": 2}");
        assertThat(map.toString(EXTERNAL_NO_SCHEMA)).isEqualTo("{\"a\": \"x\", \"b\": 2}");
        assertThat(map.toString(EXTERNAL_TOP_SCHEMA)).isEqualTo("MapOfUnknown: {\"a\": \"x\", \"b\": 2}");
        assertThat(map.toString(EXTERNAL_ALL_SCHEMA)).isEqualTo("MapOfUnknown: {\"a\": \"x\", \"b\": 2}");
    }
}
