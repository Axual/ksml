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

import java.util.Arrays;

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
import io.axual.ksml.data.type.EnumType;
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.data.type.MapType;
import io.axual.ksml.data.type.StructType;
import io.axual.ksml.data.type.UnionType;
import io.axual.ksml.data.type.UserType;
import io.axual.ksml.data.type.WindowedType;
import io.axual.ksml.exception.KSMLExecutionException;

import static io.axual.ksml.dsl.WindowedSchema.generateWindowedSchema;

public class SchemaUtil {
    private SchemaUtil() {
    }

    public static DataSchema dataTypeToSchema(DataType type) {
        if (type == DataType.UNKNOWN) return AnySchema.INSTANCE;
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

        if (type instanceof EnumType enumType)
            return new EnumSchema(null, enumType.schemaName(), "", Arrays.asList(enumType.symbols()));
        if (type instanceof ListType listType)
            return new ListSchema(dataTypeToSchema(listType.valueType()));
        // Check structs first, since they are a subclass of maps
        if (type instanceof StructType structType)
            return new StructSchema(structType.schema());
        if (type instanceof MapType mapType)
            return new MapSchema(SchemaUtil.dataTypeToSchema(mapType.valueType()));
        if (type instanceof WindowedType windowedType)
            return generateWindowedSchema(windowedType);
        if (type instanceof UnionType unionType) {
            var schemas = new DataSchema[unionType.possibleTypes().length];
            for (int index = 0; index < unionType.possibleTypes().length; index++) {
                schemas[index] = dataTypeToSchema(unionType.possibleTypes()[index].dataType());
            }
            return new UnionSchema(schemas);
        }
        throw new KSMLExecutionException("Can not convert dataType " + type + " to a schema");
    }

    public static DataType schemaToDataType(DataSchema schema) {
        if (schema == null) return DataType.UNKNOWN;
        if (schema.type() == DataSchema.Type.ANY) return DataType.UNKNOWN;
        if (schema.type() == DataSchema.Type.NULL) return DataNull.DATATYPE;
        if (schema.type() == DataSchema.Type.BOOLEAN) return DataBoolean.DATATYPE;
        if (schema.type() == DataSchema.Type.SHORT) return DataShort.DATATYPE;
        if (schema.type() == DataSchema.Type.INTEGER) return DataInteger.DATATYPE;
        if (schema.type() == DataSchema.Type.LONG) return DataLong.DATATYPE;
        if (schema.type() == DataSchema.Type.FLOAT) return DataFloat.DATATYPE;
        if (schema.type() == DataSchema.Type.DOUBLE) return DataDouble.DATATYPE;
        if (schema.type() == DataSchema.Type.BYTES) return DataBytes.DATATYPE;
        if (schema.type() == DataSchema.Type.STRING) return DataString.DATATYPE;

        if (schema instanceof EnumSchema enumSchema)
            return new EnumType(enumSchema.symbols().toArray(new String[0]));
        if (schema instanceof ListSchema listSchema)
            return new ListType(schemaToDataType(listSchema.valueSchema()));
        if (schema instanceof StructSchema structSchema) return new StructType(structSchema);
        if (schema instanceof MapSchema mapSchema)
            return new MapType(schemaToDataType(mapSchema.valueSchema()));
        if (schema instanceof UnionSchema unionSchema) {
            var types = new UserType[unionSchema.possibleSchemas().length];
            for (int index = 0; index < unionSchema.possibleSchemas().length; index++) {
                types[index] = new UserType(UserType.DEFAULT_NOTATION, schemaToDataType(unionSchema.possibleSchemas()[index]));
            }
            return new UnionType(types);
        }

        throw new KSMLExecutionException("Can not convert schema " + schema + " to a dataType");
    }
}
