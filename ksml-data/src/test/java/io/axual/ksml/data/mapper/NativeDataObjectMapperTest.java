package io.axual.ksml.data.mapper;

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

import io.axual.ksml.data.object.*;
import io.axual.ksml.data.schema.DataField;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.type.DataType;
import lombok.extern.slf4j.Slf4j;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

@Slf4j
class NativeDataObjectMapperTest {
    private final NativeDataObjectMapper mapper = new NativeDataObjectMapper();

    @Test
    @DisplayName("convertDataListToList: null DataList -> null; unwrap values including DataNull -> null")
    void convertDataListToListNullAndUnwraps() {
        log.info("start: convertDataListToListNullAndUnwraps");
        var nullList = new DataList(DataType.UNKNOWN, true);
        assertThat(mapper.convertDataListToList(nullList)).isNull();

        var list = new DataList(DataType.UNKNOWN);
        list.add(new DataString("a"));
        list.add(DataNull.INSTANCE);
        list.add(new DataInteger(3));

        var nativeList = mapper.convertDataListToList(list);
        assertThat(nativeList).containsExactly("a", null, 3);
        log.info("end: convertDataListToListNullAndUnwraps");
    }

    @Test
    @DisplayName("convertDataMapToMap: null DataMap -> null; unwrap values and sort keys alphabetically")
    void convertDataMapToMapNullAndOrdering() {
        log.info("start: convertDataMapToMapNullAndOrdering");
        var nullMap = new DataMap(DataType.UNKNOWN, true);
        assertThat(mapper.convertDataMapToMap(nullMap)).isNull();

        var map = new DataMap(DataType.UNKNOWN);
        map.put("b", new DataInteger(1));
        map.put("a", DataNull.INSTANCE);

        var nativeMap = mapper.convertDataMapToMap(map);
        assertThat(nativeMap.keySet()).containsExactly("a", "b");
        assertThat(nativeMap.get("a")).isNull();
        assertThat(nativeMap.get("b")).isEqualTo(1);
        log.info("end: convertDataMapToMapNullAndOrdering");
    }

    @Test
    @DisplayName("convertDataStructToMap: typed struct preserves required (as null if absent) and present optional fields only")
    void convertDataStructToMapTypedRequiredVsOptional() {
        log.info("start: convertDataStructToMapTypedRequiredVsOptional");
        var requiredName = new DataField("name", DataSchema.STRING_SCHEMA); // required by default
        var optionalAge = new DataField("age", DataSchema.INTEGER_SCHEMA, null, DataField.NO_TAG, false);
        var personSchema = new StructSchema("example", "Person", "doc", List.of(requiredName, optionalAge));

        var struct = new DataStruct(personSchema);
        // Only set the optional age; leave required name absent
        struct.put("age", new DataInteger(42));

        var nativeStruct = mapper.convertDataStructToMap(struct);
        // Required field must be present (null because it wasn't set); optional field included because present
        assertThat(nativeStruct).containsOnly(
                entry("name", null),
                entry("age", 42)
        );
        log.info("end: convertDataStructToMapTypedRequiredVsOptional");
    }

    @Test
    @DisplayName("convertDataStructToMap: schemaless copies all present entries as-is (unwrapped)")
    void convertDataStructToMapSchemalessCopiesAll() {
        log.info("start: convertDataStructToMapSchemalessCopiesAll");
        var struct = new DataStruct(); // schemaless
        struct.put("x", new DataString("val"));
        struct.put("y", DataNull.INSTANCE);

        var nativeStruct = mapper.convertDataStructToMap(struct);
        assertThat(nativeStruct).containsOnly(
                entry("x", "val"),
                entry("y", null)
        );
        log.info("end: convertDataStructToMapSchemalessCopiesAll");
    }

    @Test
    @DisplayName("convertDataTupleToTuple unwraps elements and preserves order")
    void convertDataTupleToTupleUnwraps() {
        log.info("start: convertDataTupleToTupleUnwraps");
        var tuple = new DataTuple(new DataString("first"), DataNull.INSTANCE, new DataInteger(7));
        var nativeTuple = mapper.convertDataTupleToTuple(tuple);
        assertThat(nativeTuple.elements()).containsExactly("first", null, 7);
    }

    @Test
    @DisplayName("fromDataObject returns native for primitives and delegates collections to helper methods")
    void fromDataObjectPrimitiveAndCollections() {
        log.info("start: fromDataObjectPrimitiveAndCollections");
        // Primitive unwrap
        assertThat(mapper.fromDataObject(new DataString("hello"))).isEqualTo("hello");
        assertThat(mapper.fromDataObject(new DataInteger(9))).isEqualTo(9);
        assertThat(mapper.fromDataObject(DataNull.INSTANCE)).isNull();

        // Collections delegate to conversion helpers we tested above
        var list = new DataList(DataType.UNKNOWN);
        list.add(new DataString("a"));
        var unwrappedList = mapper.fromDataObject(list);
        assertThat(unwrappedList).isInstanceOf(List.class);
        var asList = (List<?>) unwrappedList;
        assertThat(asList).hasSize(1);
        assertThat(asList.get(0)).isEqualTo("a");

        var map = new DataMap(DataType.UNKNOWN);
        map.put("k", new DataInteger(1));
        var unwrappedMap = mapper.fromDataObject(map);
        assertThat(unwrappedMap).isInstanceOf(Map.class);
        var asMap = (Map<?, ?>) unwrappedMap;
        assertThat(asMap).hasSize(1);
        assertThat(asMap.get("k")).isEqualTo(1);
        log.info("end: fromDataObjectPrimitiveAndCollections");
    }
}
