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

import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.exception.SchemaException;
import io.axual.ksml.data.object.*;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.type.*;
import io.axual.ksml.data.util.ConvertUtil;
import io.axual.ksml.data.util.MapUtil;
import io.axual.ksml.data.value.Tuple;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

// Maps DataObjects to/from native Java structures
public class NativeDataObjectMapper implements DataObjectMapper<Object> {
    private static final DataSchemaMapper<DataType> DATA_TYPE_DATA_SCHEMA_MAPPER = new DataTypeDataSchemaMapper();

    public DataObject toDataObject(DataType expected, Object value) {
        if (value == null) return ConvertUtil.convertNullToDataObject(expected);
        if (value instanceof DataObject val) return val;
        if (value instanceof Boolean val) return new DataBoolean(val);
        if (value instanceof Byte val) {
            if (expected == DataByte.DATATYPE) return new DataByte(val);
            if (expected == DataShort.DATATYPE) return new DataShort(val.shortValue());
            if (expected == DataInteger.DATATYPE) return new DataInteger(val.intValue());
            if (expected == DataLong.DATATYPE) return new DataLong(val.longValue());
            return new DataByte(val);
        }
        if (value instanceof Short val) {
            if (expected == DataByte.DATATYPE) return new DataByte(val.byteValue());
            if (expected == DataShort.DATATYPE) return new DataShort(val);
            if (expected == DataInteger.DATATYPE) return new DataInteger(val.intValue());
            if (expected == DataLong.DATATYPE) return new DataLong(val.longValue());
            return new DataShort(val);
        }
        if (value instanceof Integer val) {
            if (expected == DataByte.DATATYPE) return new DataByte(val.byteValue());
            if (expected == DataShort.DATATYPE) return new DataShort(val.shortValue());
            if (expected == DataInteger.DATATYPE) return new DataInteger(val);
            if (expected == DataLong.DATATYPE) return new DataLong(val.longValue());
            return new DataInteger(val);
        }
        if (value instanceof Long val) {
            if (expected == DataByte.DATATYPE) return new DataByte(val.byteValue());
            if (expected == DataShort.DATATYPE) return new DataShort(val.shortValue());
            if (expected == DataInteger.DATATYPE) return new DataInteger(val.intValue());
            if (expected == DataLong.DATATYPE) return new DataLong(val);
            return new DataLong(val);
        }
        if (value instanceof Double val) {
            if (expected == DataDouble.DATATYPE) return new DataDouble(val);
            if (expected == DataFloat.DATATYPE) return new DataFloat(val.floatValue());
            return new DataDouble(val);
        }
        if (value instanceof Float val) {
            if (expected == DataDouble.DATATYPE) return new DataDouble(val.doubleValue());
            if (expected == DataFloat.DATATYPE) return new DataFloat(val);
            return new DataFloat(val);
        }
        if (value instanceof byte[] val) return new DataBytes(val);
        if (value instanceof CharSequence val) return new DataString(val.toString());
        if (value instanceof List<?> val)
            return convertNativeToDataList(val, expected instanceof ListType expectedList ? expectedList.valueType() : DataType.UNKNOWN);
        if (value instanceof Map<?, ?> val)
            return convertNativeToDataStruct(MapUtil.stringKeys(val), expected instanceof StructType expectedStruct ? expectedStruct.schema() : null);
        if (value instanceof Tuple<?> val) return toDataTuple(val);
        throw new DataException("Can not convert value to DataObject: " + value.getClass().getSimpleName());
    }

    private DataType inferDataTypeFromNative(Object value) {
        if (value == null) return DataNull.DATATYPE;
        if (value instanceof Boolean) return DataBoolean.DATATYPE;
        if (value instanceof Byte) return DataByte.DATATYPE;
        if (value instanceof Short) return DataShort.DATATYPE;
        if (value instanceof Integer) return DataInteger.DATATYPE;
        if (value instanceof Long) return DataLong.DATATYPE;
        if (value instanceof Double) return DataDouble.DATATYPE;
        if (value instanceof Float) return DataFloat.DATATYPE;
        if (value instanceof byte[]) return DataBytes.DATATYPE;
        if (value instanceof String) return DataString.DATATYPE;
        if (value instanceof Enum<?> val) return inferEnumTypeFromNative(val);
        if (value instanceof List<?> val) return inferListTypeFromNative(val);
        if (value instanceof Map<?, ?> val) return inferStructTypeFromNative(val, null);
        if (value instanceof Tuple<?> val) return inferTupleTypeFromNative(val);
        return DataType.UNKNOWN;
    }

    private EnumType inferEnumTypeFromNative(Enum<?> value) {
        final var enumConstants = value.getClass().getEnumConstants();
        final var symbols = new ArrayList<Symbol>();
        for (final var enumConstant : enumConstants) {
            symbols.add(new Symbol(enumConstant.toString()));
        }
        return new EnumType(symbols);
    }

    private ListType inferListTypeFromNative(List<?> list) {
        // Assume the list contains all elements of the same dataType. If not validation will fail
        // later. We infer the valueType by looking at the first element of the list. If the list
        // is empty, then use dataType UNKNOWN.
        if (list.isEmpty()) return new ListType(DataType.UNKNOWN);
        return new ListType(inferDataTypeFromNative(list.getFirst()));
    }

    protected StructType inferStructTypeFromNative(Map<?, ?> map, DataSchema expected) {
        // If the expected schema is a struct schema, then return that as inferred type
        if (expected instanceof StructSchema structSchema) return new StructType(structSchema);
        // By default, return a schemaless struct type. This behaviour can be overridden in subclasses.
        return new StructType();
    }

    protected DataSchema loadSchemaByName(String schemaName) {
        throw new SchemaException("Can not load schema: " + schemaName);
    }

    private DataType inferTupleTypeFromNative(Tuple<?> tuple) {
        // Infer all subtypes
        final var subTypes = new DataType[tuple.elements().size()];
        for (int index = 0; index < tuple.elements().size(); index++) {
            subTypes[index] = inferDataTypeFromNative(tuple.elements().get(index));
        }
        return new TupleType(subTypes);
    }

    protected DataList convertNativeToDataList(List<?> list, DataType valueType) {
        final var result = new DataList(valueType);
        list.forEach(element -> result.add(toDataObject(valueType, element)));
        return result;
    }

    protected DataObject convertNativeToDataStruct(Map<String, Object> map, DataSchema schema) {
        final var type = inferStructTypeFromNative(map, schema);
        return convertNativeToDataStruct(map, type);
    }

    protected DataObject convertNativeToDataStruct(Map<String, Object> map, StructType type) {
        final var result = new DataStruct(type.schema());
        map.forEach((key, value) -> {
            final var field = type.schema() != null ? type.schema().field(key) : null;
            final var fieldSchema = field != null ? field.schema() : null;
            final var fieldType = DATA_TYPE_DATA_SCHEMA_MAPPER.fromDataSchema(fieldSchema);
            result.put(key, toDataObject(fieldType, value));
        });
        return result;
    }

    protected DataTuple toDataTuple(Tuple<?> tuple) {
        final var elements = new DataObject[tuple.elements().size()];
        for (var index = 0; index < tuple.elements().size(); index++) {
            elements[index] = toDataObject(tuple.elements().get(index));
        }
        return new DataTuple(elements);
    }

    @Override
    public Object fromDataObject(DataObject value) {
        if (value instanceof DataNull val) return val.value();

        if (value instanceof DataBoolean val) return val.value();

        if (value instanceof DataByte val) return val.value();
        if (value instanceof DataShort val) return val.value();
        if (value instanceof DataInteger val) return val.value();
        if (value instanceof DataLong val) return val.value();

        if (value instanceof DataDouble val) return val.value();
        if (value instanceof DataFloat val) return val.value();

        if (value instanceof DataBytes val) return val.value();

        if (value instanceof DataString val) return val.value();

        if (value instanceof DataEnum val) return val.value();
        if (value instanceof DataList val) return convertDataListToNative(val);
        if (value instanceof DataStruct val) return convertDataStructToNative(val);
        if (value instanceof DataTuple val) return convertDataTupleToNative(val);

        if (value instanceof DataUnion val) return fromDataObject(val.value());

        throw new DataException("Can not convert DataObject to native dataType: " + value.getClass().getSimpleName());
    }

    @Nullable
    public List<Object> convertDataListToNative(DataList list) {
        if (list.isNull()) return null;
        final var result = new ArrayList<>();
        list.forEach(element -> result.add(fromDataObject(element)));
        return result;
    }

    @Nullable
    public Map<String, Object> convertDataStructToNative(DataStruct struct) {
        if (struct.isNull()) return null;

        final var result = new TreeMap<>(DataStruct.COMPARATOR);
        struct.forEach((key, value) -> result.put(key, fromDataObject(value)));

        // Return the native representation as Map
        return result;
    }

    public Tuple<Object> convertDataTupleToNative(DataTuple value) {
        final var elements = new Object[value.elements().size()];
        for (int index = 0; index < value.elements().size(); index++) {
            elements[index] = fromDataObject(value.elements().get(index));
        }

        return new Tuple<>(elements);
    }
}
