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

import io.axual.ksml.data.object.DataBoolean;
import io.axual.ksml.data.object.DataByte;
import io.axual.ksml.data.object.DataBytes;
import io.axual.ksml.data.object.DataDouble;
import io.axual.ksml.data.object.DataFloat;
import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataLong;
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataShort;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.data.type.MapType;
import io.axual.ksml.data.type.StructType;
import io.axual.ksml.data.type.UnionType;
import io.axual.ksml.data.type.WindowedType;
import io.axual.ksml.exception.KSMLDataException;
import io.axual.ksml.data.mapper.WindowedSchemaMapper;

public class SchemaUtil {
    private static final WindowedSchemaMapper mapper = new WindowedSchemaMapper();

    private SchemaUtil() {
    }

    public static DataSchema dataTypeToSchema(DataType type) {
        if (type == DataNull.DATATYPE) return DataSchema.create(DataSchema.Type.NULL);
        if (type == DataBoolean.DATATYPE) return DataSchema.create(DataSchema.Type.BOOLEAN);
        if (type == DataByte.DATATYPE) return DataSchema.create(DataSchema.Type.BYTE);
        if (type == DataShort.DATATYPE) return DataSchema.create(DataSchema.Type.SHORT);
        if (type == DataInteger.DATATYPE) return DataSchema.create(DataSchema.Type.INTEGER);
        if (type == DataLong.DATATYPE) return DataSchema.create(DataSchema.Type.LONG);
        if (type == DataFloat.DATATYPE) return DataSchema.create(DataSchema.Type.FLOAT);
        if (type == DataDouble.DATATYPE) return DataSchema.create(DataSchema.Type.DOUBLE);
        if (type == DataBytes.DATATYPE) return DataSchema.create(DataSchema.Type.BYTES);
        if (type == DataString.DATATYPE) return DataSchema.create(DataSchema.Type.STRING);
        if (type instanceof ListType listType)
            return new ListSchema(dataTypeToSchema(listType.valueType()));
        // Check structs first, since they are a subclass of maps
        if (type instanceof StructType structType)
            return new StructSchema(structType.schema());
        if (type instanceof MapType mapType)
            return new MapSchema(SchemaUtil.dataTypeToSchema(mapType.valueType()));
        if (type instanceof WindowedType windowedType)
            return mapper.toDataSchema(windowedType);
        if (type instanceof UnionType unionType) {
            var schemas = new DataSchema[unionType.possibleTypes().length];
            for (int index = 0; index < unionType.possibleTypes().length; index++) {
                schemas[index] = dataTypeToSchema(unionType.possibleTypes()[index].dataType());
            }
            return new UnionSchema(schemas);
        }
        throw new KSMLDataException("Can not convert data dataType " + type + " to schema dataType");
    }
}
