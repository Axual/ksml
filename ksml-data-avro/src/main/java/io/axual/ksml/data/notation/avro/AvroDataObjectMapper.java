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

import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.type.DataType;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

public class AvroDataObjectMapper extends NativeDataObjectMapper {
    private static final AvroSchemaMapper AVRO_SCHEMA_MAPPER = new AvroSchemaMapper();

    protected AvroDataObjectMapper() {
        super(true);
    }

    @Override
    public DataObject toDataObject(DataType expected, Object value) {
        if (value == null || value == JsonProperties.NULL_VALUE)
            return NativeDataObjectMapper.convertFromNull(expected);
        if (value instanceof Utf8 utf8) return new DataString(utf8.toString());
        if (value instanceof GenericData.EnumSymbol) {
            return new DataString(value.toString());
        }
        if (value instanceof GenericRecord rec) {
            final var schema = rec.getSchema();
            DataStruct result = new DataStruct(AVRO_SCHEMA_MAPPER.toDataSchema(schema.getNamespace(), schema.getName(), schema));
            for (Schema.Field field : schema.getFields()) {
                result.put(field.name(), toDataObject(rec.get(field.name())));
            }
            return result;
        }
        return super.toDataObject(expected, value);
    }

    @Override
    public Object fromDataObject(DataObject value) {
        if (value instanceof DataNull) return null;
        if (value instanceof DataStruct struct) {
            if (struct.isNull()) return null;
            return new AvroObject(struct.type().schema(), fromDataStruct(struct));
        }
        return super.fromDataObject(value);
    }
}
