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
import io.axual.ksml.data.type.StructType;
import io.axual.ksml.data.type.TupleType;
import io.axual.ksml.data.type.UserType;
import io.axual.ksml.parser.UserTypeParser;

import java.util.stream.Collectors;

public class UserTypeStringMapper {
    public String toString(UserType value) {
        var notation = value.notation().toLowerCase();
        var type = toString(value.dataType());
        if (type == null || type.isEmpty()) return notation;
        return notation + ":" + type;
    }

    private String toString(DataType value) {
        if (value == DataNull.DATATYPE) return "null";
        if (value == DataBoolean.DATATYPE) return "boolean";
        if (value == DataByte.DATATYPE) return "byte";
        if (value == DataShort.DATATYPE) return "short";
        if (value == DataInteger.DATATYPE) return "int";
        if (value == DataLong.DATATYPE) return "long";
        if (value == DataDouble.DATATYPE) return "double";
        if (value == DataFloat.DATATYPE) return "float";
        if (value == DataBytes.DATATYPE) return "bytes";
        if (value == DataString.DATATYPE) return "string";
        if (value instanceof EnumType enumType) return enumTypeToString(enumType);
        if (value instanceof ListType listType) return listTypeToString(listType);
        if (value instanceof StructType structType) return structTypeToString(structType);
        if (value instanceof TupleType tupleType) return tupleTypeToString(tupleType);
        return null;
    }

    private String enumTypeToString(EnumType enumType) {
        return "enum(" + String.join(",", enumType.getSymbols()) + ")";
    }

    private String listTypeToString(ListType listType) {
        return "[" + toString(listType.valueType()) + "]";
    }

    private String structTypeToString(StructType structType) {
        if (structType.schema() == null) return "struct";
        return structType.schema().name();
    }

    private String tupleTypeToString(TupleType tupleType) {
        var types = tupleType.subTypes().stream().map(this::toString).collect(Collectors.joining(","));
        return "(" + types + ")";
    }

    public UserType fromString(String type) {
        return UserTypeParser.parse(type);
    }
}
