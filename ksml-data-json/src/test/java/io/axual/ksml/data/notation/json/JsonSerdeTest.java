package io.axual.ksml.data.notation.json;

/*-
 * ========================LICENSE_START=================================
 * KSML
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

import com.fasterxml.jackson.databind.ObjectMapper;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.object.DataList;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.data.type.StructType;
import io.axual.ksml.data.type.UnionType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link JsonSerde} covering round-trips for Struct/List/Union, null handling,
 * and failure on incorrect input types.
 */
@DisplayName("JsonSerde - serializer/deserializer behavior")
class JsonSerdeTest {
    private static final ObjectMapper JACKSON = new ObjectMapper();
    private static final JsonDataObjectMapper DATA_MAPPER = new JsonDataObjectMapper(false);

    @Test
    @DisplayName("StructType: native Map round-trip to DataStruct and equal JSON tree")
    void structRoundTrip() throws Exception {
        var expectedType = new StructType();
        var serde = new JsonSerde(new NativeDataObjectMapper(), expectedType);

        // Build native Map with insertion order preserved for deterministic JSON field order
        Map<String, Object> nativeMap = new LinkedHashMap<>();
        nativeMap.put("a", 1);
        nativeMap.put("b", "x");
        Map<String, Object> inner = new LinkedHashMap<>();
        inner.put("y", true);
        nativeMap.put("c", inner);

        // Serialize -> bytes; then deserialize -> DataObject
        var bytes = serde.serializer().serialize("topic", nativeMap);
        var out = serde.deserializer().deserialize("topic", bytes);
        assertThat(out).isInstanceOf(DataStruct.class);

        // Compare JSON trees for semantic equality
        var jsonOut = DATA_MAPPER.fromDataObject((DataObject) out);
        var expected = JACKSON.createObjectNode();
        expected.put("a", 1);
        expected.put("b", "x");
        expected.set("c", JACKSON.createObjectNode().put("y", true));
        var treeOut = JACKSON.readTree(jsonOut);
        assertThat(treeOut).isEqualTo(expected);
    }

    @Test
    @DisplayName("ListType: native List round-trip to DataList and equal JSON tree")
    void listRoundTrip() throws Exception {
        var expectedType = new ListType();
        var serde = new JsonSerde(new NativeDataObjectMapper(), expectedType);

        List<Object> list = new ArrayList<>();
        list.add(1);
        list.add("x");
        list.add(Map.of("k", "v"));

        var bytes = serde.serializer().serialize("topic", list);
        var out = serde.deserializer().deserialize("topic", bytes);
        assertThat(out).isInstanceOf(DataList.class);

        var jsonOut = DATA_MAPPER.fromDataObject((DataObject) out);
        var tree = JACKSON.readTree(jsonOut);
        // Build expected tree and compare
        var expected = JACKSON.readTree("[1,\"x\",{\"k\":\"v\"}]");
        assertThat(tree).isEqualTo(expected);
    }

    @Test
    @DisplayName("UnionType(Struct|List): supports both object and array inputs")
    void unionRoundTrip() {
        DataType union = new UnionType(
                new UnionType.MemberType(new StructType()),
                new UnionType.MemberType(new ListType())
        );
        var serde = new JsonSerde(new NativeDataObjectMapper(), union);

        var softly = new SoftAssertions();

        // Object
        Map<String, Object> obj = Map.of("a", 1);
        var b1 = serde.serializer().serialize("t", obj);
        var d1 = serde.deserializer().deserialize("t", b1);
        softly.assertThat(d1).isInstanceOf(DataStruct.class);

        // Array
        List<Object> arr = List.of(1, 2, 3);
        var b2 = serde.serializer().serialize("t", arr);
        var d2 = serde.deserializer().deserialize("t", b2);
        softly.assertThat(d2).isInstanceOf(DataList.class);

        softly.assertAll();
    }

    @Test
    @DisplayName("Null round-trip yields DataNull")
    void nullRoundTrip() {
        var serde = new JsonSerde(new NativeDataObjectMapper(), new StructType());
        var bytes = serde.serializer().serialize("t", null);
        var out = serde.deserializer().deserialize("t", bytes);
        assertThat(out).asInstanceOf(InstanceOfAssertFactories.type(DataStruct.class))
                .returns(true, DataStruct::isNull);
    }

    @Test
    @DisplayName("Incorrect input type for expected StructType fails")
    void wrongTypeSerializationThrows() {
        var serde = new JsonSerde(new NativeDataObjectMapper(), new StructType());
        var serializer = serde.serializer();
        assertThatThrownBy(() -> serializer.serialize("t", 5))
                .isInstanceOf(DataException.class);
    }
}
