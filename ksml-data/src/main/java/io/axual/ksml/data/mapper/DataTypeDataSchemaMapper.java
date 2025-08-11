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

import java.util.List;

/**
 * Maps between KSML DataType and DataSchema representations.
 * This implementation provides a bidirectional conversion used throughout the
 * data layer to describe and materialize schemas from types and vice versa.
 */
public class DataTypeDataSchemaMapper implements DataSchemaMapper<DataType> {
    /**
     * Converts a DataType into a DataSchema.
     *
     * @param namespace optional namespace to use for named schema types, may be null
     * @param name      optional name to use for named schema types, may be null
     * @param type      the DataType to convert
     * @return the resulting DataSchema
     * @throws SchemaException if the data type can not be converted into a schema
     */
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
        if (type instanceof MapType mapType)
            return new MapSchema(toDataSchema(namespace, name, mapType.valueType()));
        if (type instanceof StructType structType)
            return structType.schema() != null ? new StructSchema(structType.schema()) : StructSchema.SCHEMALESS;
        if (type instanceof TupleType tupleType)
            return new TupleSchema(tupleType, this);
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

    /**
     * Converts a DataSchema into a DataType.
     *
     * @param schema the DataSchema to convert
     * @return the resulting DataType
     * @throws SchemaException if the schema can not be converted into a data type
     */
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
        if (schema instanceof MapSchema mapSchema) return new MapType(fromDataSchema(mapSchema.valueSchema()));
        // Process TupleSchema first, since it inherits from StructSchema
        if (schema instanceof TupleSchema tupleSchema)
            return new TupleType(convertFieldsToSubTypes(tupleSchema.fields()));
        if (schema instanceof StructSchema structSchema) return new StructType(structSchema);
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

    private DataType[] convertFieldsToSubTypes(List<DataField> fields) {
        var result = new DataType[fields.size()];
        for (int index = 0; index < fields.size(); index++) {
            result[index] = fromDataSchema(fields.get(index).schema());
        }
        return result;
    }
}
