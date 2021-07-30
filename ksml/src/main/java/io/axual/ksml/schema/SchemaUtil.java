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

import io.axual.ksml.data.object.DataBoolean;
import io.axual.ksml.data.object.DataByte;
import io.axual.ksml.data.object.DataBytes;
import io.axual.ksml.data.object.DataDouble;
import io.axual.ksml.data.object.DataFloat;
import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataLong;
import io.axual.ksml.data.object.DataShort;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.type.DataListType;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.RecordType;
import io.axual.ksml.data.type.WindowedType;
import io.axual.ksml.exception.KSMLExecutionException;

public class SchemaUtil {
    private SchemaUtil() {
    }

    public static Schema dataTypeToSchema(DataType type) {
        if (type == DataBoolean.TYPE) return Schema.create(Schema.Type.BOOLEAN);
        if (type == DataByte.TYPE) return Schema.create(Schema.Type.INT);
        if (type == DataShort.TYPE) return Schema.create(Schema.Type.INT);
        if (type == DataInteger.TYPE) return Schema.create(Schema.Type.INT);
        if (type == DataLong.TYPE) return Schema.create(Schema.Type.LONG);
        if (type == DataFloat.TYPE) return Schema.create(Schema.Type.FLOAT);
        if (type == DataDouble.TYPE) return Schema.create(Schema.Type.DOUBLE);
        if (type == DataBytes.TYPE) return Schema.create(Schema.Type.BYTES);
        if (type == DataString.TYPE) return Schema.create(Schema.Type.STRING);
        if (type instanceof DataListType)
            return Schema.createArray(dataTypeToSchema(((DataListType) type).valueType()));
        if (type instanceof RecordType) return (((RecordType) type).schema().schema());
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
