package io.axual.ksml.python;

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

import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.notation.SchemaResolver;
import io.axual.ksml.data.object.DataBoolean;
import io.axual.ksml.data.object.DataByte;
import io.axual.ksml.data.object.DataBytes;
import io.axual.ksml.data.object.DataDouble;
import io.axual.ksml.data.object.DataFloat;
import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataList;
import io.axual.ksml.data.object.DataLong;
import io.axual.ksml.data.object.DataMap;
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataShort;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.object.DataTuple;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.data.type.MapType;
import io.axual.ksml.data.type.StructType;
import io.axual.ksml.data.type.TupleType;
import io.axual.ksml.data.type.UnionType;
import io.axual.ksml.data.util.MapUtil;
import io.axual.ksml.exception.ExecutionException;
import io.axual.ksml.execution.ExecutionContext;
import io.axual.ksml.util.ExecutionUtil;
import org.graalvm.polyglot.Value;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

public class PythonDataObjectMapper extends NativeDataObjectMapperWithSchema {
    private static final SchemaResolver<DataSchema> SCHEMA_RESOLVER = schemaName -> ExecutionContext.INSTANCE.schemaLibrary().getSchema(schemaName, false);

    public PythonDataObjectMapper(boolean includeSchemaInfo, NativeDataObjectMapper recursiveDataObjectMapper) {
        super(SCHEMA_RESOLVER, includeSchemaInfo, recursiveDataObjectMapper);
    }

    @Override
    public DataObject toDataObject(DataType expected, Object object) {
        if (object instanceof Value value) {
            // If we got a polyglot Value object, then convert it below before letting the remainder be
            // handled by the superclass
            object = polyglotValueToNative(expected, value);
        }

        // If we expect a union dataType, then check its value types, else convert by value.
        if (expected instanceof UnionType unionType)
            return polyglotValueToDataUnion(unionType, object);

        // Finally, convert all native types to DataObjects
        return super.toDataObject(expected, object);
    }

    private DataObject polyglotValueToDataUnion(UnionType unionType, Object value) {
        final var memberTypesInitialScan = new ArrayList<>(Arrays.asList(unionType.members()));
        // Remove any null memberTypes and scan the other types
        final var hasNullType = memberTypesInitialScan.removeIf(memberType -> DataNull.DATATYPE.equals(memberType.type()));
        for (final var memberType : memberTypesInitialScan) {
            try {
                var result = toDataObject(memberType.type(), value);
                if (result != null) return result;
            } catch (Exception e) {
                // Ignore exception and move to next value type
            }
        }
        // Handle the nullType scenario
        if (hasNullType) {
            var result = toDataObject(DataNull.DATATYPE, value);
            if (result != null) return result;
        }

        final var sourceValue = toDataObject(value);
        final var sourceValueStr = sourceValue != null ? sourceValue.toString(DataObject.Printer.EXTERNAL_NO_SCHEMA) : DataNull.INSTANCE.toString();
        final var sourceType = sourceValue != null ? sourceValue.type() : DataType.UNKNOWN;
        throw new DataException("Can not convert " + sourceType + " to " + unionType + ": value=" + sourceValueStr);
    }

    private Object polyglotValueToNative(DataType expected, Value object) {
        if (object.isNull()) return null;
        if (object.isBoolean() && (expected == null || expected == DataBoolean.DATATYPE))
            return object.asBoolean();

        if (object.isNumber()) return polyglotNumberToNative(expected, object);

        if (object.isString()) return object.asString();

        if (object.hasArrayElements()) {
            final var result = polyglotArrayToDataObject(expected, object);
            if (result != null) return result;
        }

        // By default, try to decode a dict as a struct
        if (object.hasHashEntries()) {
            final var result = polyglotMapToDataObject(expected, object);
            if (result != null) return result;
        }

        throw new DataException("Can not convert Python dataType to DataObject: "
                + object.getClass().getSimpleName()
                + (expected != null ? ", expected: " + expected : ""));
    }

    private Object polyglotNumberToNative(DataType expected, Value object) {
        if (expected != null) {
            if (expected == DataByte.DATATYPE) return object.asByte();
            if (expected == DataShort.DATATYPE) return object.asShort();
            if (expected == DataInteger.DATATYPE) return object.asInt();
            if (expected == DataLong.DATATYPE) return object.asLong();
            if (expected == DataFloat.DATATYPE) return object.asFloat();
            if (expected == DataDouble.DATATYPE) return object.asDouble();
        }
        // Return a long by default
        return object.asLong();
    }

    private Object polyglotArrayToDataObject(DataType expected, Value object) {
        if (expected == DataBytes.DATATYPE) {
            final var bytes = new byte[(int) object.getArraySize()];
            for (var index = 0; index < object.getArraySize(); index++) {
                bytes[index] = (byte) object.getArrayElement(index).asInt();
            }
            return bytes;
        }
        if (expected instanceof TupleType expectedTuple) {
            final var elements = new DataObject[(int) object.getArraySize()];
            for (var index = 0; index < object.getArraySize(); index++) {
                var subType = expectedTuple.subType(index);
                elements[index] = toDataObject(subType, object.getArrayElement(index));
            }
            return new DataTuple(elements);
        }
        if (expected == null || expected instanceof ListType) {
            final var valueType = expected != null ? ((ListType) expected).valueType() : DataType.UNKNOWN;
            final var result = new DataList(valueType);
            for (var index = 0; index < object.getArraySize(); index++) {
                result.add(toDataObject(valueType, object.getArrayElement(index)));
            }
            return result;
        }
        return null;
    }

    private DataObject polyglotMapToDataObject(DataType expected, Value object) {
        final var map = ExecutionUtil.tryThis(() -> object.as(Map.class));
        if (map == null) return null;
        if (expected instanceof MapType expectedMapType)
            return convertMapToDataMap(MapUtil.stringKeys(map), expectedMapType);
        return convertMapToDataStruct(MapUtil.stringKeys(map), expected instanceof StructType structType ? structType.schema() : null);
    }

    @Override
    public Value fromDataObject(DataObject object) {
        if (object instanceof DataNull) return Value.asValue(null);
        if (object instanceof DataBoolean val) return Value.asValue(val.value());
        if (object instanceof DataByte val) return Value.asValue(val.value());
        if (object instanceof DataShort val) return Value.asValue(val.value());
        if (object instanceof DataInteger val) return Value.asValue(val.value());
        if (object instanceof DataLong val) return Value.asValue(val.value());
        if (object instanceof DataFloat val) return Value.asValue(val.value());
        if (object instanceof DataDouble val) return Value.asValue(val.value());
        if (object instanceof DataBytes val) {
            // Convert the contained byte array to a list of unsigned bytes (as short), so it can be converted to a
            // Python list by the PythonFunction wrapper code downstream...
            final var values = new ArrayList<Short>(val.value().length);
            for (byte b : val.value()) values.add(b >= 0 ? (short) b : (short) (256 + b));
            return Value.asValue(values);
        }
        if (object instanceof DataString val) return Value.asValue(val.value());
        if (object instanceof DataList val) return Value.asValue(convertDataListToList(val));
        if (object instanceof DataMap val) return Value.asValue(convertDataMapToMap(val));
        if (object instanceof DataStruct val) return Value.asValue(convertDataStructToMap(val));
        throw new ExecutionException("Can not convert DataObject to Python dataType: " + object.getClass().getSimpleName());
    }
}
