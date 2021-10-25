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

import io.axual.ksml.data.mapper.NativeUserObjectMapper;
import io.axual.ksml.data.object.user.UserObject;
import io.axual.ksml.data.object.user.UserRecord;
import io.axual.ksml.data.object.user.UserString;
import io.axual.ksml.data.type.user.UserType;
import io.axual.ksml.schema.SchemaUtil;

import static io.axual.ksml.data.type.user.UserType.DEFAULT_NOTATION;

public class AvroDataMapper extends NativeUserObjectMapper {
    @Override
    public UserObject toUserObject(UserType expected, Object value) {
        final String resultNotation = expected != null ? expected.notation() : DEFAULT_NOTATION;
        if (value instanceof Utf8) return new UserString(resultNotation, ((Utf8) value).toString());
        if (value instanceof GenericData.EnumSymbol) {
            return new UserString(resultNotation, value.toString());
        }
        if (value instanceof GenericRecord) {
            GenericRecord record = (GenericRecord) value;
            UserRecord result = new UserRecord(SchemaUtil.schemaToDataSchema(record.getSchema()));
            for (Schema.Field field : record.getSchema().getFields()) {
                result.put(field.name(), toUserObject(resultNotation, record.get(field.name())));
            }
            return result;
        }

        return super.toUserObject(expected, value);
    }

    @Override
    public Object fromUserObject(UserObject value) {
        if (value instanceof UserRecord) {
            var rec = (UserRecord) value;
            return new AvroObject(SchemaUtil.dataSchemaToAvroSchema(rec.type().schema()), userRecordToMap(rec));
        }
        return super.fromUserObject(value);
    }
}
