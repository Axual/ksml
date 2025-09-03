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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.annotation.Nullable;

import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.exception.SchemaException;
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
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.EnumType;
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.data.type.MapType;
import io.axual.ksml.data.type.StructType;
import io.axual.ksml.data.type.Symbol;
import io.axual.ksml.data.type.TupleType;
import io.axual.ksml.data.util.ConvertUtil;
import io.axual.ksml.data.util.MapUtil;
import io.axual.ksml.data.value.Tuple;
import lombok.extern.slf4j.Slf4j;

/**
 * Maps DataObjects to and from native Java structures (Object, Map, List, primitives, etc.).
 * This mapper infers types when necessary and can use an expected DataType to guide conversions.
 */
@Slf4j
public class NativeDataObjectMapper implements DataObjectMapper<Object> {
    private static final DataTypeDataSchemaMapper DATA_TYPE_DATA_SCHEMA_MAPPER = new DataTypeDataSchemaMapper();
    private final SchemaResolver<DataSchema> schemaResolver;
    private final ConvertUtil converter;

    /**
     * Creates a mapper without a SchemaResolver. Schema names that need resolution will cause errors.
     */
    public NativeDataObjectMapper() {
        this(null);
    }

    /**
     * Creates a mapper with a provided SchemaResolver for resolving schema names to DataSchema instances.
     *
     * @param schemaResolver resolver used to fetch schemas by name; may be null
     */
    public NativeDataObjectMapper(SchemaResolver<DataSchema> schemaResolver) {
        this.schemaResolver = schemaResolver;
        this.converter = new ConvertUtil(this, DATA_TYPE_DATA_SCHEMA_MAPPER);
    }

    /**
     * Converts a native Java value to a DataObject, attempting to coerce it to the expected DataType when provided.
     *
     * @param expected the expected DataType, or null if unknown
     * @param value    the native value to convert
     * @return the DataObject representation of the value
     */
    public DataObject toDataObject(DataType expected, Object value) {
        if (value instanceof CharSequence val) value = val.toString();
        var result = convertObjectToDataObject(expected, value);
        if (expected != null && !expected.isAssignableFrom(result))
            result = converter.convert(null, null, expected, result, false);
        return result;
    }

    private DataObject convertObjectToDataObject(DataType expected, Object value) {
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
            return convertListToDataList(val, expected instanceof ListType expectedList ? expectedList.valueType() : DataType.UNKNOWN);
        if (value instanceof Map<?, ?> val) {
            if (expected instanceof MapType expectedMapType) {
                return convertMapToDataMap(MapUtil.stringKeys(val), expectedMapType);
            }
            if (expected instanceof StructType expectedStruct) {
                return convertMapToDataStruct(MapUtil.stringKeys(val), expectedStruct.schema());
            } else {
                log.debug("Ignoring exptected type {} for conversion", expected);
                return convertMapToDataStruct(MapUtil.stringKeys(val), (DataSchema) null);
            }
        }
        if (value instanceof Tuple<?> val) return convertTupleToDataTuple(val);
        throw new DataException("Can not convert value to DataObject: " + value.getClass().getSimpleName());
    }

    private DataType inferDataTypeFromObject(Object value) {
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
        if (value instanceof Enum<?> val) return inferEnumTypeFromEnum(val);
        if (value instanceof List<?> val) return inferListTypeFromList(val);
        if (value instanceof Map<?, ?> val) return inferDataTypeFromNativeMap(val, null);
        if (value instanceof Tuple<?> val) return inferTupleTypeFromTuple(val);
        return DataType.UNKNOWN;
    }

    private EnumType inferEnumTypeFromEnum(Enum<?> value) {
        final var enumConstants = value.getClass().getEnumConstants();
        final var symbols = new ArrayList<Symbol>();
        for (final var enumConstant : enumConstants) {
            symbols.add(new Symbol(enumConstant.toString()));
        }
        return new EnumType(symbols);
    }

    private ListType inferListTypeFromList(List<?> list) {
        // Assume the list contains all elements of the same dataType. If not validation will fail
        // later. We infer the valueType by looking at the first element of the list. If the list
        // is empty, then use dataType UNKNOWN.
        if (list.isEmpty()) return new ListType(DataType.UNKNOWN);
        return new ListType(inferDataTypeFromObject(list.getFirst()));
    }

    protected DataType inferDataTypeFromNativeMap(Map<?, ?> map, DataSchema expected) {
        // If the expected schema is a struct schema, then return that as inferred type
        if (expected instanceof StructSchema structSchema) return new StructType(structSchema);
        // By default, return a schemaless struct type. This behaviour can be overridden in subclasses.
        // TODO: this should be MapType(), but leaving this step for now, as we may switch anytime in the future
        return new StructType();
    }

    protected DataSchema loadSchemaByName(String schemaName) {
        if (schemaResolver != null) {
            final var result = schemaResolver.get(schemaName);
            if (result != null) return result;
        }
        throw new SchemaException("Can not load schema: " + schemaName);
    }

    private DataType inferTupleTypeFromTuple(Tuple<?> tuple) {
        // Infer all subtypes
        final var subTypes = new DataType[tuple.elements().size()];
        for (int index = 0; index < tuple.elements().size(); index++) {
            subTypes[index] = inferDataTypeFromObject(tuple.elements().get(index));
        }
        return new TupleType(subTypes);
    }

    protected DataList convertListToDataList(List<?> list, DataType valueType) {
        final var result = new DataList(valueType);
        list.forEach(element -> {
            var dataObject = toDataObject(valueType, element);
            if (!valueType.isAssignableFrom(dataObject))
                dataObject = converter.convert(null, null, valueType, dataObject, false);
            result.add(toDataObject(valueType, dataObject));
        });
        return result;
    }

    protected DataObject convertMapToDataMap(Map<String, Object> map, MapType targetType) {
        final var result = new DataMap(targetType.valueType());
        map.forEach((key, value) -> result.put(key, toDataObject(targetType.valueType(), value)));
        return result;
    }

    protected DataObject convertMapToDataStruct(Map<String, Object> map, DataSchema schema) {
        final var type = inferDataTypeFromNativeMap(map, schema);
        if (type instanceof MapType mapType) return convertMapToDataMap(map, mapType);
        if (type instanceof StructType structType) return convertMapToDataStruct(map, structType);
        throw new DataException("Can not convert map to DataObject: " + type.getClass().getSimpleName());
    }

    protected DataObject convertMapToDataStruct(Map<String, Object> map, StructType type) {
        final var result = new DataStruct(type.schema());
        map.forEach((key, value) -> {
            final var field = type.schema() != null ? type.schema().field(key) : null;
            final var fieldSchema = field != null ? field.schema() : null;
            final var fieldType = DATA_TYPE_DATA_SCHEMA_MAPPER.fromDataSchema(fieldSchema);
            result.put(key, toDataObject(fieldType, value));
        });
        return result;
    }

    protected DataTuple convertTupleToDataTuple(Tuple<?> tuple) {
        final var elements = new DataObject[tuple.elements().size()];
        for (var index = 0; index < tuple.elements().size(); index++) {
            elements[index] = toDataObject(tuple.elements().get(index));
        }
        return new DataTuple(elements);
    }

    /**
     * Converts a DataObject back to its native Java representation (Object, Map, List, primitives, etc.).
     *
     * @param value the DataObject to convert
     * @return the native Java value
     */
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

        if (value instanceof DataList val) return convertDataListToList(val);
        if (value instanceof DataMap val) return convertDataMapToMap(val);
        if (value instanceof DataStruct val) return convertDataStructToMap(val);
        if (value instanceof DataTuple val) return convertDataTupleToTuple(val);

        throw new DataException("Can not convert DataObject to native dataType: " + (value != null ? value.getClass().getSimpleName() : "null"));
    }

    /**
     * Converts a DataList to a native Java List.
     *
     * @param list the DataList to convert
     * @return a List of native values, or null if the DataList represents null
     */
    @Nullable
    public List<Object> convertDataListToList(DataList list) {
        if (list.isNull()) return null;
        final var result = new ArrayList<>();
        list.forEach(element -> result.add(fromDataObject(element)));
        return result;
    }

    /**
     * Converts a DataMap to a native Java Map with String keys.
     *
     * @param map the DataMap to convert
     * @return a Map of native values, or null if the DataMap represents null
     */
    @Nullable
    public Map<String, Object> convertDataMapToMap(DataMap map) {
        if (map.isNull()) return null;
        final var result = new TreeMap<String, Object>();
        map.forEach((key, value) -> result.put(key, fromDataObject(value)));
        return result;
    }

    /**
     * Converts a DataStruct to a native Java Map. For typed structs this preserves required fields
     * and includes only present optional fields; schemaless structs copy all entries.
     *
     * @param struct the DataStruct to convert
     * @return a Map of native values, or null if the DataStruct represents null
     */
    @Nullable
    public Map<String, Object> convertDataStructToMap(DataStruct struct) {
        if (struct.isNull()) return null;
        final var result = new TreeMap<>(DataStruct.COMPARATOR);
        if (struct.type().schema() instanceof StructSchema structSchema) {
            for (final var field : structSchema.fields()) {
                final var key = field.name();
                final var value = struct.get(key) != null ? fromDataObject(struct.get(key)) : null;
                // Copy the field when required, is explicitly contained in the struct
                if (field.required() || struct.containsKey(key))
                    result.put(key, value);
            }
        } else {
            // Copy all fields to the map
            struct.forEach((key, value) -> result.put(key, fromDataObject(value)));
        }

        // Return the native representation as Map
        return result;
    }

    /**
     * Converts a DataTuple to a native Tuple<Object>.
     *
     * @param value the DataTuple to convert
     * @return a Tuple containing native values
     */
    public Tuple<Object> convertDataTupleToTuple(DataTuple value) {
        final var elements = new Object[value.elements().size()];
        for (int index = 0; index < value.elements().size(); index++) {
            elements[index] = fromDataObject(value.elements().get(index));
        }

        return new Tuple<>(elements);
    }
}
