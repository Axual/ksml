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
import io.axual.ksml.data.type.EnumType;
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.data.type.MapType;
import io.axual.ksml.data.type.RecordType;
import io.axual.ksml.data.type.WindowedType;
import io.axual.ksml.exception.KSMLExecutionException;
import io.axual.ksml.schema.mapper.WindowedSchemaMapper;

public class SchemaUtil {
    private static final WindowedSchemaMapper mapper = new WindowedSchemaMapper();
    private static final String[] stringArray = new String[0];

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
        if (type instanceof MapType mapType)
            return new MapSchema(SchemaUtil.dataTypeToSchema(mapType.valueType()));
        if (type instanceof RecordType recordType)
            return new RecordSchema(recordType.schema());
        if (type instanceof WindowedType windowedType)
            return mapper.toDataSchema(windowedType);
        throw new KSMLExecutionException("Can not convert data dataType " + type + " to schema dataType");
    }

//    public static DataType schemaToDataType(DataSchema schema) {
//        return switch (schema.dataType()) {
//            case NULL -> DataNull.DATATYPE;
//            case BOOLEAN -> DataBoolean.DATATYPE;
//            case BYTE -> DataByte.DATATYPE;
//            case BYTES, FIXED -> DataBytes.DATATYPE;
//            case SHORT -> DataShort.DATATYPE;
//            case DOUBLE -> DataDouble.DATATYPE;
//            case FLOAT -> DataFloat.DATATYPE;
//            case INTEGER -> DataInteger.DATATYPE;
//            case LONG -> DataLong.DATATYPE;
//            case STRING -> DataString.DATATYPE;
//            case ENUM -> schemaToEnumType((EnumSchema) schema);
//            case LIST -> new ListType(schemaToDataType(((ListSchema) schema).valueType()));
//            case MAP -> new MapType();
//            case RECORD -> new RecordType((RecordSchema) schema);
//            case UNION -> schemaToUnionType((UnionSchema) schema);
//        };
//    }
//
    public static EnumType schemaToEnumType(EnumSchema schema) {
        return new EnumType(schema.name(), schema.possibleValues().toArray(stringArray));
    }

//    private static UnionType schemaToUnionType(UnionSchema schema) {
//        var possibleTypes = new DataType[schema.possibleSchema().length];
//        for (int index = 0; index < possibleTypes.length; index++) {
//            possibleTypes[index] = schemaToDataType(schema.possibleSchema()[index]);
//        }
//        return new UnionType(possibleTypes);
//    }
}
