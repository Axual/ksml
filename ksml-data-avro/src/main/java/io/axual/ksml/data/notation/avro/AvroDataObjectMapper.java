package io.axual.ksml.data.notation.avro;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2023 Axual B.V.
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

import io.axual.ksml.data.mapper.DataTypeDataSchemaMapper;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.object.*;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.util.ConvertUtil;
import org.apache.avro.JsonProperties;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import java.nio.ByteBuffer;

/**
 * DataObject mapper for Avro-native values.
 *
 * <p>This mapper adapts native Avro runtime objects to KSML DataObject instances and back.
 * It adds Avro-specific handling on top of NativeDataObjectMapper:
 * - Maps Avro Utf8 to DataString.
 * - Maps ByteBuffer to bytes (DataBytes) on the way in.
 * - Maps Avro GenericRecord to DataStruct using AvroSchemaMapper for schema bridging.
 * - Maps a DataStruct back to an AvroObject wrapper so downstream Avro serdes can validate against schema.</p>
 *
 * <p>See ksml-data/DEVELOPER_GUIDE.md for details about DataObject, DataType, and schema mapping.</p>
 */
public class AvroDataObjectMapper extends NativeDataObjectMapper {
    private static final AvroSchemaMapper AVRO_SCHEMA_MAPPER = new AvroSchemaMapper();
    private static final DataTypeDataSchemaMapper TYPE_SCHEMA_MAPPER = new DataTypeDataSchemaMapper();

    /**
     * Convert a native Avro value to a KSML DataObject, honoring an expected DataType when provided.
     *
     * <p>Special cases:
     * - null or Avro's JsonProperties.NULL_VALUE -> typed null via ConvertUtil.convertNullToDataObject(expected)
     * - Utf8 -> DataString
     * - ByteBuffer -> treat as byte[]
     * - GenericRecord -> DataStruct built from its Avro schema using AvroSchemaMapper</p>
     *
     * @param expected the expected target KSML DataType (may be null)
     * @param value    the native value coming from Avro serialization/deserialization
     * @return a DataObject representing the input, possibly a typed-null container when expected is provided
     */
    @Override
    public DataObject toDataObject(DataType expected, Object value) {
        if (value == null || value == JsonProperties.NULL_VALUE)
            return ConvertUtil.convertNullToDataObject(expected);
        if (value instanceof Utf8 utf8) return new DataString(utf8.toString());
        if (value instanceof GenericData.EnumSymbol) {
            return new DataString(value.toString());
        }
        if(value instanceof ByteBuffer buffer) {
            return super.toDataObject(expected, buffer.array());
        }
        if (value instanceof GenericRecord rec) {
            final var avroSchema = rec.getSchema();
            final var dataSchema = AVRO_SCHEMA_MAPPER.toDataSchema(avroSchema.getNamespace(), avroSchema.getName(), avroSchema);
            final var result = new DataStruct(dataSchema);
            for (final var field : dataSchema.fields()) {
                final var key = field.name();
                final var dataObject = toDataObject(TYPE_SCHEMA_MAPPER.fromDataSchema(field.schema()), rec.get(key));
                final var isNull = dataObject == DataNull.INSTANCE
                        || (dataObject instanceof DataList list && list.isNull())
                        || (dataObject instanceof DataStruct struct && struct.isNull());
                if (field.required() || !isNull)
                    result.put(key, dataObject);
            }
            return result;
        }
        return super.toDataObject(expected, value);
    }

    /**
     * Convert a KSML DataObject back to a native value suitable for Avro serdes.
     *
     * <p>Special cases:
     * - DataNull or null-struct/list containers -> null
     * - DataStruct with schema -> AvroObject wrapper that validates values against the derived Avro schema</p>
     *
     * @param value the DataObject to convert
     * @return a native value (including AvroObject for structs) or null
     */
    @Override
    public Object fromDataObject(DataObject value) {
        if (value instanceof DataNull) return null;
        if (value instanceof DataStruct struct) {
            if (struct.isNull()) return null;
            return new AvroObject(struct.type().schema(), convertDataStructToMap(struct));
        }
        return super.fromDataObject(value);
    }
}
