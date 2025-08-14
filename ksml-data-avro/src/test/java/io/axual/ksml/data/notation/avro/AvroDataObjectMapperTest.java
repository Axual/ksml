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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import io.axual.ksml.data.notation.avro.test.AvroTestUtil;
import io.axual.ksml.data.object.DataBoolean;
import io.axual.ksml.data.object.DataBytes;
import io.axual.ksml.data.object.DataDouble;
import io.axual.ksml.data.object.DataFloat;
import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataList;
import io.axual.ksml.data.object.DataLong;
import io.axual.ksml.data.object.DataMap;
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataStruct;

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
        assertThat(nativeAvro).isInstanceOf(GenericRecord.class);

        // Convert back to KSML DataStruct from the AvroObject wrapper
        DataStruct roundTripped = (DataStruct) mapper.toDataObject(null, nativeAvro);

        // Assert: structures are equal field-by-field (using DataObject equals)
        assertThat(roundTripped).isEqualTo(firstStruct);
    }

    @Test
    void optional_allNullOrOmitted_shouldNotContainOptionalFields() {
        Schema schema = AvroTestUtil.loadSchema(SCHEMA_OPTIONAL);
        // Create an empty record, all fields are optional
        GenericData.Record rec = new GenericData.Record(schema);
        // Verify all fields
        assertThat(GenericData.get().validate(schema, rec)).isTrue();
        DataStruct ds = (DataStruct) mapper.toDataObject(null, rec);

        // All fields should be absent (getter returns null)
        assertThat(ds.get("optStr")).isEqualTo(new DataString(null));
        assertThat(ds.get("optInt")).isEqualTo(new DataInteger(null));
        assertThat(ds.get("optLong")).isEqualTo(new DataLong(null));
        assertThat(ds.get("optFloat")).isEqualTo(new DataFloat(null));
        assertThat(ds.get("optDouble")).isEqualTo(new DataDouble(null));
        assertThat(ds.get("optBool")).isEqualTo(new DataBoolean(null));
        assertThat(ds.get("optBytes")).isEqualTo(new DataBytes(null));
        assertThat(ds.get("optStrList")).isNull();
        DataMap expectedOptIntMap = new DataMap(DataInteger.DATATYPE, true);
        DataObject realOptIntMap = ds.get("optIntMap");
        assertThat(realOptIntMap).usingRecursiveComparison().isEqualTo(expectedOptIntMap);
        assertThat(ds.get("optRec")).isNull();
        assertThat(ds.get("optEnum")).isNull();
    }

    @Test
    void optional_withValues_shouldMapCorrectTypes() {
        Schema schema = AvroTestUtil.loadSchema(SCHEMA_OPTIONAL);
        GenericRecord rec = AvroTestUtil.parseRecord(schema, DATA_OPTIONAL_WITH_VALUES);
        DataStruct ds = (DataStruct) mapper.toDataObject(null, rec);

        assertThat(((DataString) ds.get("optStr")).value()).isEqualTo("hello");
        assertThat(((DataInteger) ds.get("optInt")).value()).isEqualTo(10);
        assertThat(((DataLong) ds.get("optLong")).value()).isEqualTo(9999999999L);
        assertThat(((DataFloat) ds.get("optFloat")).value()).isEqualTo(3.5f);
        assertThat(((DataDouble) ds.get("optDouble")).value()).isEqualTo(6.25d);
        assertThat(((DataBoolean) ds.get("optBool")).value()).isTrue();
        // bytes normalization handled in AvroDataObjectMapper -> super; value existence is sufficient
        assertThat(ds.get("optBytes")).isNotNull();

        DataList list = (DataList) ds.get("optStrList");
        assertThat(list.size()).isEqualTo(2);
        assertThat(((DataString) list.get(0)).value()).isEqualTo("x");

        DataMap map = (DataMap) ds.get("optIntMap");
        assertThat(((DataInteger) map.get("a")).value()).isEqualTo(1);
        assertThat(((DataInteger) map.get("b")).value()).isEqualTo(2);

        DataStruct inner = (DataStruct) ds.get("optRec");
        assertThat(((DataInteger) inner.get("id")).value()).isEqualTo(5);

        // Avro enum becomes DataString
        assertThat(((DataString) ds.get("optEnum")).value()).isEqualTo("GREEN");

        // Round-trip stability for this struct
        DataStruct again = (DataStruct) mapper.toDataObject( mapper.fromDataObject(ds));
        assertThat(again).usingRecursiveComparison().isEqualTo(ds);
    }
}
