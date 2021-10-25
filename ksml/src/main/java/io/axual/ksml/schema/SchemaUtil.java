package io.axual.ksml.schema;

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

import io.axual.ksml.data.object.user.UserBoolean;
import io.axual.ksml.data.object.user.UserByte;
import io.axual.ksml.data.object.user.UserBytes;
import io.axual.ksml.data.object.user.UserDouble;
import io.axual.ksml.data.object.user.UserFloat;
import io.axual.ksml.data.object.user.UserInteger;
import io.axual.ksml.data.object.user.UserLong;
import io.axual.ksml.data.object.user.UserShort;
import io.axual.ksml.data.object.user.UserString;
import io.axual.ksml.data.type.user.UserListType;
import io.axual.ksml.data.type.base.DataType;
import io.axual.ksml.data.type.user.UserRecordType;
import io.axual.ksml.data.type.base.WindowedType;
import io.axual.ksml.exception.KSMLExecutionException;

public class SchemaUtil {
    private SchemaUtil() {
    }

    public static Schema dataTypeToSchema(DataType type) {
        if (type == UserBoolean.TYPE) return Schema.create(Schema.Type.BOOLEAN);
        if (type == UserByte.TYPE) return Schema.create(Schema.Type.INT);
        if (type == UserShort.TYPE) return Schema.create(Schema.Type.INT);
        if (type == UserInteger.TYPE) return Schema.create(Schema.Type.INT);
        if (type == UserLong.TYPE) return Schema.create(Schema.Type.LONG);
        if (type == UserFloat.TYPE) return Schema.create(Schema.Type.FLOAT);
        if (type == UserDouble.TYPE) return Schema.create(Schema.Type.DOUBLE);
        if (type == UserBytes.TYPE) return Schema.create(Schema.Type.BYTES);
        if (type == UserString.TYPE) return Schema.create(Schema.Type.STRING);
        if (type instanceof UserListType)
            return Schema.createArray(dataTypeToSchema(((UserListType) type).valueType().type()));
        if (type instanceof UserRecordType) return (((UserRecordType) type).schema().schema());
        if (type instanceof WindowedType) return WindowedSchema.generateWindowedSchema((WindowedType) type);
        throw new KSMLExecutionException("Can not convert data type " + type + " to schema type");
    }

    public static DataSchema windowTypeToSchema(WindowedType type) {
        return DataSchema.newBuilder(WindowedSchema.generateWindowedSchema(type)).build();
    }

    public static Schema dataSchemaToAvroSchema(DataSchema schema) {
        return schema.schema();
    }

    public static DataSchema schemaToDataSchema(Schema schema) {
        return DataSchema.newBuilder(schema).build();
    }

    public static DataSchema parse(String schemaStr) {
        var schema = new Schema.Parser().parse(schemaStr);
        return DataSchema.newBuilder(schema).build();
    }
}
