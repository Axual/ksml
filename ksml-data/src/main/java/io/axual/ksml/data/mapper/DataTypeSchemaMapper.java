package io.axual.ksml.data.mapper;

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

import io.axual.ksml.data.exception.ExecutionException;
import io.axual.ksml.data.object.*;
import io.axual.ksml.data.schema.*;
import io.axual.ksml.data.type.*;

public class DataTypeSchemaMapper implements DataSchemaMapper<DataType> {
    public DataSchema toDataSchema(String namespace, String name, DataType type) {
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
            return new EnumSchema(null, enumType.schemaName(), "", enumType.symbols());
        if (type instanceof ListType listType)
            return new ListSchema(toDataSchema(listType.valueType()));
        // Check structs first, since they are a subclass of maps
        if (type instanceof StructType structType)
            return structType.schema() != null ? new StructSchema(structType.schema()) : new StructSchema();
        if (type instanceof MapType mapType)
            return new MapSchema(toDataSchema(namespace, name, mapType.valueType()));
        if (type instanceof UnionType unionType) {
            var fields = new DataField[unionType.valueTypes().length];
            for (int index = 0; index < unionType.valueTypes().length; index++) {
                final var valueType = unionType.valueTypes()[index];
                fields[index] = new DataField(
                        valueType.name(),
                        toDataSchema(valueType.type()),
                        null,
                        valueType.index());
            }
            return new UnionSchema(fields);
        }
        throw new ExecutionException("Can not convert dataType " + type + " to a schema");
    }

    public DataType fromDataSchema(DataSchema schema) {
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
            return new EnumType(enumSchema.symbols());
        if (schema instanceof ListSchema listSchema)
            return new ListType(fromDataSchema(listSchema.valueSchema()));
        if (schema instanceof StructSchema structSchema) return new StructType(structSchema);
        if (schema instanceof MapSchema mapSchema)
            return new MapType(fromDataSchema(mapSchema.valueSchema()));
        if (schema instanceof UnionSchema unionSchema) {
            var types = new UnionType.ValueType[unionSchema.valueTypes().length];
            for (int index = 0; index < unionSchema.valueTypes().length; index++) {
                final var valueType = unionSchema.valueTypes()[index];
                types[index] = new UnionType.ValueType(
                        valueType.name(),
                        fromDataSchema(valueType.schema()),
                        valueType.index());
            }
            return new UnionType(types);
        }

        throw new ExecutionException("Can not convert schema " + schema + " to a dataType");
    }
}
