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

import io.axual.ksml.data.exception.SchemaException;
import io.axual.ksml.data.object.*;
import io.axual.ksml.data.schema.*;
import io.axual.ksml.data.type.*;

public class DataTypeDataSchemaMapper implements DataSchemaMapper<DataType> {
    public DataSchema toDataSchema(String namespace, String name, DataType type) {
        if (type == DataType.UNKNOWN) return DataSchema.ANY_SCHEMA;
        if (type == DataNull.DATATYPE) return DataSchema.NULL_SCHEMA;
        if (type == DataBoolean.DATATYPE) return DataSchema.BOOLEAN_SCHEMA;
        if (type == DataByte.DATATYPE) return DataSchema.BYTE_SCHEMA;
        if (type == DataShort.DATATYPE) return DataSchema.SHORT_SCHEMA;
        if (type == DataInteger.DATATYPE) return DataSchema.INTEGER_SCHEMA;
        if (type == DataLong.DATATYPE) return DataSchema.LONG_SCHEMA;
        if (type == DataFloat.DATATYPE) return DataSchema.FLOAT_SCHEMA;
        if (type == DataDouble.DATATYPE) return DataSchema.DOUBLE_SCHEMA;
        if (type == DataBytes.DATATYPE) return DataSchema.BYTES_SCHEMA;
        if (type == DataString.DATATYPE) return DataSchema.STRING_SCHEMA;

        if (type instanceof EnumType enumType)
            return new EnumSchema(null, enumType.name(), "", enumType.symbols());
        if (type instanceof ListType listType)
            return new ListSchema(toDataSchema(listType.valueType()));
        // Check structs first, since they are a subclass of maps
        if (type instanceof StructType structType)
            return structType.schema() != null ? new StructSchema(structType.schema()) : new StructSchema();
        if (type instanceof MapType mapType)
            return new MapSchema(toDataSchema(namespace, name, mapType.valueType()));
        if (type instanceof UnionType unionType) {
            var fields = new DataField[unionType.memberTypes().length];
            for (int index = 0; index < unionType.memberTypes().length; index++) {
                final var memberType = unionType.memberTypes()[index];
                fields[index] = new DataField(
                        memberType.name(),
                        toDataSchema(memberType.type()),
                        null,
                        memberType.tag());
            }
            return new UnionSchema(fields);
        }
        throw new SchemaException("Can not convert dataType " + type + " to a schema");
    }

    public DataType fromDataSchema(DataSchema schema) {
        if (schema == null) return DataType.UNKNOWN;
        if (schema == DataSchema.ANY_SCHEMA) return DataType.UNKNOWN;
        if (schema == DataSchema.NULL_SCHEMA) return DataNull.DATATYPE;
        if (schema == DataSchema.BOOLEAN_SCHEMA) return DataBoolean.DATATYPE;
        if (schema == DataSchema.SHORT_SCHEMA) return DataShort.DATATYPE;
        if (schema == DataSchema.INTEGER_SCHEMA) return DataInteger.DATATYPE;
        if (schema == DataSchema.LONG_SCHEMA) return DataLong.DATATYPE;
        if (schema == DataSchema.FLOAT_SCHEMA) return DataFloat.DATATYPE;
        if (schema == DataSchema.DOUBLE_SCHEMA) return DataDouble.DATATYPE;
        if (schema == DataSchema.BYTES_SCHEMA) return DataBytes.DATATYPE;
        if (schema == DataSchema.STRING_SCHEMA) return DataString.DATATYPE;
        if (schema instanceof EnumSchema enumSchema) return new EnumType(enumSchema.symbols());
        if (schema instanceof ListSchema listSchema) return new ListType(fromDataSchema(listSchema.valueSchema()));
        if (schema instanceof StructSchema structSchema) return new StructType(structSchema);
        if (schema instanceof MapSchema mapSchema) return new MapType(fromDataSchema(mapSchema.valueSchema()));
        if (schema instanceof UnionSchema unionSchema) {
            var types = new UnionType.MemberType[unionSchema.memberSchemas().length];
            for (int index = 0; index < unionSchema.memberSchemas().length; index++) {
                final var memberSchema = unionSchema.memberSchemas()[index];
                types[index] = new UnionType.MemberType(
                        memberSchema.name(),
                        fromDataSchema(memberSchema.schema()),
                        memberSchema.tag());
            }
            return new UnionType(types);
        }

        throw new SchemaException("Can not convert schema " + schema + " to a dataType");
    }
}
