package io.axual.ksml.notation.avro;

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

import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import io.axual.ksml.notation.binary.NativeDataObjectMapper;
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.type.DataType;

public class AvroDataMapper extends NativeDataObjectMapper {
    @Override
    public DataObject toDataObject(DataType expected, Object value) {
        if (value == null || value == JsonProperties.NULL_VALUE) return DataNull.INSTANCE;
        if (value instanceof Utf8 utf8) return new DataString(utf8.toString());
        if (value instanceof GenericData.EnumSymbol) {
            return new DataString(value.toString());
        }
        if (value instanceof GenericRecord rec) {
            DataStruct result = new DataStruct(new AvroSchemaMapper().toDataSchema(rec.getSchema().getName(), rec.getSchema()));
            for (Schema.Field field : rec.getSchema().getFields()) {
                result.put(field.name(), toDataObject(rec.get(field.name())));
            }
            return result;
        }

        return super.toDataObject(expected, value);
    }

    @Override
    public Object fromDataObject(DataObject value) {
        if (value instanceof DataNull) return null;
        if (value instanceof DataStruct rec) {
            return new AvroObject(rec.type().schema(), fromDataStruct(rec));
        }
        return super.fromDataObject(value);
    }
}
