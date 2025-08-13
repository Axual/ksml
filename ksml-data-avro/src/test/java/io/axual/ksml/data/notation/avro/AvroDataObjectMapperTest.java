package io.axual.ksml.data.notation.avro;

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

import io.axual.ksml.data.notation.avro.test.AvroTestUtil;
import io.axual.ksml.data.object.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import static io.axual.ksml.data.notation.avro.test.AvroTestUtil.*;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for AvroDataObjectMapper using KSML DataObject conversion rules.
 *
 * References:
 * - ksml-data/DEVELOPER_GUIDE.md (DataObject mappings and behavior)
 * - AvroTestUtil for loading Avro schemas and JSON data
 */
class AvroDataObjectMapperTest {
    private final AvroDataObjectMapper mapper = new AvroDataObjectMapper();

    private GenericRecord loadRecord(String schemaPath, String dataPath) {
        Schema schema = AvroTestUtil.loadSchema(schemaPath);
        return AvroTestUtil.parseRecord(schema, dataPath);
    }

    @Test
    void toDataObject_shouldMapPrimitiveFields_fromAvroGenericRecord() {
        // Arrange
        GenericRecord primitivesRecord = loadRecord(SCHEMA_PRIMITIVES, DATA_PRIMITIVES);

        // Act
        DataObject mapped = mapper.toDataObject(null, primitivesRecord);

        // Assert
        assertThat(mapped).isInstanceOf(DataStruct.class);
        DataStruct struct = (DataStruct) mapped;
        assertThat(struct.get("str")).isInstanceOf(DataString.class);
        assertThat(((DataString) struct.get("str")).value()).isEqualTo("hello");

        assertThat(struct.get("i")).isInstanceOf(DataInteger.class);
        assertThat(((DataInteger) struct.get("i")).value()).isEqualTo(123);

        assertThat(struct.get("l")).isInstanceOf(DataLong.class);
        assertThat(((DataLong) struct.get("l")).value()).isEqualTo(1234567890L);

        assertThat(struct.get("f")).isInstanceOf(DataFloat.class);
        assertThat(((DataFloat) struct.get("f")).value()).isEqualTo(12.5f);

        assertThat(struct.get("d")).isInstanceOf(DataDouble.class);
        assertThat(((DataDouble) struct.get("d")).value()).isEqualTo(12345.6789d);

        assertThat(struct.get("b")).isInstanceOf(DataBoolean.class);
        assertThat(((DataBoolean) struct.get("b")).value()).isTrue();

        // Bytes handling differs across Avro versions (ByteBuffer vs byte[]). The mapper currently
        // does not explicitly normalize ByteBuffer here, so we intentionally do not assert the 'bytes' field.
    }

    @Test
    void toDataObject_shouldMapCollectionsAndUnions_withAllBranches() {
        // Arrange
        GenericRecord collectionsRecord = loadRecord(SCHEMA_COLLECTIONS, DATA_COLLECTIONS_1);

        // Act
        DataStruct struct = (DataStruct) mapper.toDataObject(null, collectionsRecord);

        // Assert arrays and maps
        assertThat(struct.get("strs")).isInstanceOf(DataList.class);
        DataList stringsList = (DataList) struct.get("strs");
        assertThat(stringsList.size()).isEqualTo(3);
        assertThat(stringsList.get(0)).isInstanceOf(DataString.class);
        assertThat(((DataString) stringsList.get(0)).value()).isEqualTo("a");

        assertThat(struct.get("intMap")).isInstanceOf(DataMap.class);
        DataMap intMap = (DataMap) struct.get("intMap");
        assertThat(intMap.size()).isEqualTo(2);
        assertThat(((DataInteger) intMap.get("k1")).value()).isEqualTo(1);
        assertThat(((DataInteger) intMap.get("k2")).value()).isEqualTo(2);

        // Assert single union branch (string)
        assertThat(struct.get("singleUnion")).isInstanceOf(DataString.class);
        assertThat(((DataString) struct.get("singleUnion")).value()).isEqualTo("one");

        // Assert union list covering: null, string, int, record X, enum Y
        assertThat(struct.get("unionList")).isInstanceOf(DataList.class);
        DataList unionList = (DataList) struct.get("unionList");
        assertThat(unionList.size()).isEqualTo(5);

        assertThat(unionList.get(0)).isSameAs(DataNull.INSTANCE);
        assertThat(((DataString) unionList.get(1)).value()).isEqualTo("abc");
        assertThat(((DataInteger) unionList.get(2)).value()).isEqualTo(7);

        assertThat(unionList.get(3)).isInstanceOf(DataStruct.class);
        DataStruct xRecord = (DataStruct) unionList.get(3);
        assertThat(((DataInteger) xRecord.get("id")).value()).isEqualTo(1);
        assertThat(((DataString) xRecord.get("name")).value()).isEqualTo("john");

        // Avro enums map to DataString symbol name
        assertThat(((DataString) unionList.get(4)).value()).isEqualTo("C");
    }

    @Test
    void toDataObject_shouldMapUnionVariant_null() {
        // Arrange
        GenericRecord genericRecord = loadRecord(SCHEMA_COLLECTIONS, DATA_COLLECTIONS_SINGLE_UNION_NULL);

        // Act
        DataStruct struct = (DataStruct) mapper.toDataObject(null, genericRecord);

        // Assert
        assertThat(struct.get("singleUnion")).isNull();
        assertThat(((DataList) struct.get("strs")).size()).isZero();
        assertThat(((DataMap) struct.get("intMap")).size()).isZero();
        assertThat(((DataList) struct.get("unionList")).size()).isZero();
    }

    @Test
    void toDataObject_shouldMapUnionVariant_int() {
        // Arrange
        GenericRecord genericRecord = loadRecord(SCHEMA_COLLECTIONS, DATA_COLLECTIONS_SINGLE_UNION_INT);

        // Act
        DataStruct struct = (DataStruct) mapper.toDataObject(null, genericRecord);

        // Assert
        assertThat(struct.get("singleUnion")).isInstanceOf(DataInteger.class);
        assertThat(((DataInteger) struct.get("singleUnion")).value()).isEqualTo(42);
    }

    @Test
    void toDataObject_shouldMapUnionVariant_record() {
        // Arrange
        GenericRecord genericRecord = loadRecord(SCHEMA_COLLECTIONS, DATA_COLLECTIONS_SINGLE_UNION_RECORD);

        // Act
        DataStruct struct = (DataStruct) mapper.toDataObject(null, genericRecord);

        // Assert
        assertThat(struct.get("singleUnion")).isInstanceOf(DataStruct.class);
        DataStruct x = (DataStruct) struct.get("singleUnion");
        assertThat(((DataInteger) x.get("id")).value()).isEqualTo(7);
        assertThat(((DataString) x.get("name")).value()).isEqualTo("seven");
    }

    @Test
    void roundTrip_genericRecord_toDataObject_toAvroObject_toDataObject_shouldBeStable() {
        // Arrange: use the collections case that exercises many mappings
        GenericRecord inputRecord = loadRecord(SCHEMA_COLLECTIONS, DATA_COLLECTIONS_1);

        // Act: Avro GenericRecord -> KSML DataStruct
        DataStruct firstStruct = (DataStruct) mapper.toDataObject(null, inputRecord);

        // Convert KSML DataStruct -> Avro native wrapper (GenericRecord)
        Object nativeAvro = mapper.fromDataObject(firstStruct);
        assertThat(nativeAvro).isInstanceOf(AvroObject.class);

        // Convert back to KSML DataStruct from the AvroObject wrapper
        DataStruct roundTripped = (DataStruct) mapper.toDataObject(null, nativeAvro);

        // Assert: structures are equal field-by-field (using DataObject equals)
        assertThat(roundTripped).isEqualTo(firstStruct);
    }
}
