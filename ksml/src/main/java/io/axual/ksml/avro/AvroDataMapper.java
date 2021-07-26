package io.axual.ksml.avro;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 Axual B.V.
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
import org.apache.avro.util.Utf8;

import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataRecord;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.mapper.NativeDataMapper;
import io.axual.ksml.util.SchemaUtil;

public class AvroDataMapper extends NativeDataMapper {
    @Override
    public DataObject toDataObject(Object value) {
        if (value instanceof Utf8) return new DataString(((Utf8) value).toString());
        if (value instanceof GenericData.EnumSymbol) {
            return new DataString(value.toString());
        }
        if (value instanceof GenericRecord) {
            GenericRecord record = (GenericRecord) value;
            DataRecord result = new DataRecord(SchemaUtil.schemaToDataSchema(record.getSchema()));
            for (Schema.Field field : record.getSchema().getFields()) {
                result.put(field.name(), toDataObject(record.get(field.name())));
            }
            return result;
        }

        return super.toDataObject(value);
    }

    @Override
    public Object fromDataObject(DataObject value) {
        if (value instanceof DataRecord) {
            var rec = (DataRecord) value;
            return new AvroObject(SchemaUtil.dataSchemaToAvroSchema(rec.type().schema()), dataRecordToMap(rec));
        }
        return super.fromDataObject(value);
    }
}
