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
import io.axual.ksml.data.object.DataBoolean;
import io.axual.ksml.data.object.DataByte;
import io.axual.ksml.data.object.DataBytes;
import io.axual.ksml.data.object.DataDouble;
import io.axual.ksml.data.object.DataEnum;
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
import io.axual.ksml.data.schema.MapSchema;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.data.type.MapType;
import io.axual.ksml.data.type.StructType;
import io.axual.ksml.data.type.TupleType;
import io.axual.ksml.data.util.ConvertUtil;
import io.axual.ksml.data.util.MapUtil;
import io.axual.ksml.data.value.Struct;
import io.axual.ksml.data.value.Tuple;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Bidirectional mapper between KSML {@link io.axual.ksml.data.object.DataObject} values and
 * native Java representations (primitives and their wrappers, String, byte[], List, Map, and Tuple).
 *
 * <p>Responsibilities:</p>
 * <ul>
 *   <li>Convert native Java values to {@code DataObject} instances, optionally guided by an expected {@link io.axual.ksml.data.type.DataType}.</li>
 *   <li>Infer {@link io.axual.ksml.data.type.DataType} information from native values (lists, maps, tuples, primitives).</li>
 *   <li>Convert {@code DataObject} instances back to native Java forms.</li>
 *   <li>Resolve schemas via an optional {@link io.axual.ksml.data.notation.SchemaResolver} when field types reference schema names.</li>
 * </ul>
 *
 * <p>For complex types, conversion and inference are performed recursively. When an expected
 * {@code DataType} is provided but the produced value is not directly assignable, a compatibility
 * conversion is attempted via {@link io.axual.ksml.data.util.ConvertUtil}.</p>
 */
@Slf4j
public class NativeDataObjectMapper implements DataObjectMapper<Object> {
    private static final DataTypeDataSchemaMapper DATA_TYPE_DATA_SCHEMA_MAPPER = new DataTypeDataSchemaMapper();
    private final ConvertUtil converter;

    /**
     * Constructor for a new mapper.
     */
    public NativeDataObjectMapper() {
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
        if (expected != null && expected.isAssignableFrom(result).isNotAssignable())
            result = converter.convert(null, null, expected, result, false);
        return result;
    }

    /**
     * Internal conversion from a native Java value to a {@link DataObject} without post-conversion compatibility checks.
     *
     * <p>Handles primitives, wrappers, Strings, byte arrays, Lists, Maps, and Tuples. For collections,
     * this method delegates to specialized helpers that may use the provided expected type to guide
     * element conversion.</p>
     *
     * @param expected optional expected {@link DataType} used to steer coercion of numeric widths and collections
     * @param value    native Java value to convert
     * @return the corresponding {@code DataObject}
     * @throws io.axual.ksml.data.exception.DataException if the value type cannot be represented
     */
    private DataObject convertObjectToDataObject(DataType expected, Object value) {
        if (value == null) return ConvertUtil.convertNullToDataObject(expected);
        if (value instanceof DataObject val) return val;
        if (value instanceof Boolean val) {
            if (expected == null || expected == DataType.UNKNOWN || expected == DataBoolean.DATATYPE)
                return new DataBoolean(val);
        }
        if (value instanceof Byte val) {
            if (expected == null || expected == DataType.UNKNOWN || expected == DataByte.DATATYPE)
                return new DataByte(val);
            if (expected == DataShort.DATATYPE) return new DataShort(val.shortValue());
            if (expected == DataInteger.DATATYPE) return new DataInteger(val.intValue());
            if (expected == DataLong.DATATYPE) return new DataLong(val.longValue());
            if (expected == DataFloat.DATATYPE) return new DataFloat(val.floatValue());
            if (expected == DataDouble.DATATYPE) return new DataDouble(val.doubleValue());
        }
        if (value instanceof Short val) {
            if (expected == DataByte.DATATYPE) return new DataByte(val.byteValue());
            if (expected == null || expected == DataType.UNKNOWN || expected == DataShort.DATATYPE)
                return new DataShort(val);
            if (expected == DataInteger.DATATYPE) return new DataInteger(val.intValue());
            if (expected == DataLong.DATATYPE) return new DataLong(val.longValue());
            if (expected == DataFloat.DATATYPE) return new DataFloat(val.floatValue());
            if (expected == DataDouble.DATATYPE) return new DataDouble(val.doubleValue());
        }
        if (value instanceof Integer val) {
            if (expected == DataByte.DATATYPE) return new DataByte(val.byteValue());
            if (expected == DataShort.DATATYPE) return new DataShort(val.shortValue());
            if (expected == null || expected == DataType.UNKNOWN || expected == DataInteger.DATATYPE)
                return new DataInteger(val);
            if (expected == DataLong.DATATYPE) return new DataLong(val.longValue());
            if (expected == DataFloat.DATATYPE) return new DataFloat(val.floatValue());
            if (expected == DataDouble.DATATYPE) return new DataDouble(val.doubleValue());
        }
        if (value instanceof Long val) {
            if (expected == DataByte.DATATYPE) return new DataByte(val.byteValue());
            if (expected == DataShort.DATATYPE) return new DataShort(val.shortValue());
            if (expected == DataInteger.DATATYPE) return new DataInteger(val.intValue());
            if (expected == null || expected == DataType.UNKNOWN || expected == DataLong.DATATYPE)
                return new DataLong(val);
            if (expected == DataFloat.DATATYPE) return new DataFloat(val.floatValue());
            if (expected == DataDouble.DATATYPE) return new DataDouble(val.doubleValue());
        }
        if (value instanceof Double val) {
            if (expected == DataByte.DATATYPE) return new DataByte(val.byteValue());
            if (expected == DataShort.DATATYPE) return new DataShort(val.shortValue());
            if (expected == DataInteger.DATATYPE) return new DataInteger(val.intValue());
            if (expected == DataLong.DATATYPE) return new DataLong(val.longValue());
            if (expected == null || expected == DataType.UNKNOWN || expected == DataDouble.DATATYPE)
                return new DataDouble(val);
            if (expected == DataFloat.DATATYPE) return new DataFloat(val.floatValue());
        }
        if (value instanceof Float val) {
            if (expected == DataByte.DATATYPE) return new DataByte(val.byteValue());
            if (expected == DataShort.DATATYPE) return new DataShort(val.shortValue());
            if (expected == DataInteger.DATATYPE) return new DataInteger(val.intValue());
            if (expected == DataLong.DATATYPE) return new DataLong(val.longValue());
            if (expected == DataDouble.DATATYPE) return new DataDouble(val.doubleValue());
            if (expected == null || expected == DataType.UNKNOWN || expected == DataFloat.DATATYPE)
                return new DataFloat(val);
        }
        if (value instanceof byte[] val) {
            if (expected instanceof ListType expectedListType)
                return convertByteArrayToList(val, expectedListType.valueType());
            if (expected == null || expected == DataType.UNKNOWN || expected == DataBytes.DATATYPE)
                return new DataBytes(val);
        }
        if (value instanceof CharSequence val) return new DataString(val.toString());
        if (value instanceof Tuple<?> val) return convertTupleToDataTuple(val);
        if (value instanceof List<?> val) {
            if (expected == DataBytes.DATATYPE) return convertListToDataBytes(val);
            if (expected instanceof TupleType expectedTupleType) return convertListToDataTuple(val, expectedTupleType);
            return convertListToDataList(val, expected instanceof ListType expectedList ? expectedList.valueType() : DataType.UNKNOWN);
        }
        if (value instanceof Map<?, ?> val) {
            if (expected instanceof MapType expectedMapType) {
                return convertMapToDataMap(MapUtil.stringKeys(val), expectedMapType);
            }
            if (expected instanceof StructType expectedStruct) {
                return convertMapToDataStruct(MapUtil.stringKeys(val), expectedStruct.schema());
            } else {
                log.debug("Ignoring expected type {} for conversion", expected);
                return convertMapToDataStruct(MapUtil.stringKeys(val), (DataSchema) null);
            }
        }
        if (value instanceof Tuple<?> val) return convertTupleToDataTuple(val);
        throw new DataException("Can not convert value to DataObject: " + value.getClass().getSimpleName());
    }

    /**
     * Infer a {@link DataType} from a native Java value.
     *
     * <p>Numbers are mapped to their corresponding primitive types, collections and tuples are
     * delegated to dedicated inference helpers, and unknown values result in {@link DataType#UNKNOWN}.</p>
     *
     * @param value native Java value to inspect
     * @return the inferred {@code DataType}
     */
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
        if (value instanceof List<?> val) return inferListTypeFromList(val);
        if (value instanceof Map<?, ?> val) return inferDataTypeFromNativeMap(val, null);
        if (value instanceof Tuple<?> val) return inferTupleTypeFromTuple(val);
        return DataType.UNKNOWN;
    }

    /**
     * Infer a {@link ListType} from a native list by inspecting the first element.
     *
     * <p>If the list is empty, the value type is {@link DataType#UNKNOWN}. The method assumes
     * homogeneous element types; validation elsewhere may reject heterogeneous lists.</p>
     *
     * @param list the source list
     * @return the inferred list type
     */
    private ListType inferListTypeFromList(List<?> list) {
        // Assume the list contains all elements of the same dataType. If not validation will fail
        // later. We infer the valueType by looking at the first element of the list. If the list
        // is empty, then use dataType UNKNOWN.
        if (list.isEmpty()) return new ListType(DataType.UNKNOWN);
        return new ListType(inferDataTypeFromObject(list.getFirst()));
    }

    /**
     * Infer a {@link DataType} from a native Map, optionally guided by an expected {@link DataSchema}.
     *
     * <p>By default, a provided {@link StructSchema} leads to a {@link StructType} of that schema; otherwise
     * this implementation returns a schemaless {@link StructType}. Subclasses may override to produce
     * a {@link MapType} instead or to apply notation-specific rules.</p>
     *
     * @param map      the source map (keys are converted to String elsewhere when needed)
     * @param expected an expected schema, possibly null
     * @return the inferred {@code DataType}
     */
    protected DataType inferDataTypeFromNativeMap(Map<?, ?> map, DataSchema expected) {
        // If the expected schema is a map schema, then return that as the inferred type
        if (expected instanceof MapSchema mapSchema)
            return new MapType(DATA_TYPE_DATA_SCHEMA_MAPPER.fromDataSchema(mapSchema.valueSchema()));
        // If the expected schema is a struct schema, then return that as the inferred type
        if (expected instanceof StructSchema structSchema) return new StructType(structSchema);
        // By default, return a schemaless struct type
        return new StructType();
    }

    /**
     * Infer a {@link TupleType} from a native {@link Tuple} by inferring all element subtypes.
     *
     * @param list the source list
     * @return the inferred tuple type
     */
    private TupleType inferTupleTypeFromList(List<?> list) {
        // Infer all subtypes
        final var subTypes = new DataType[list.size()];
        for (int index = 0; index < list.size(); index++) {
            subTypes[index] = inferDataTypeFromObject(list.get(index));
        }
        return new TupleType(subTypes);
    }

    /**
     * Infer a {@link TupleType} from a native {@link Tuple} by inferring all element subtypes.
     *
     * @param tuple the source tuple
     * @return the inferred tuple type
     */
    private TupleType inferTupleTypeFromTuple(Tuple<?> tuple) {
        // Infer all subtypes
        final var subTypes = new DataType[tuple.elements().size()];
        for (int index = 0; index < tuple.elements().size(); index++) {
            subTypes[index] = inferDataTypeFromObject(tuple.elements().get(index));
        }
        return new TupleType(subTypes);
    }

    protected DataList convertByteArrayToList(byte[] bytes, DataType valueType) {
        final var result = new DataList(valueType);
        for (var index = 0; index < bytes.length; index++) {
            result.add(new DataByte(bytes[index]));
        }
        return result;
    }

    protected DataBytes convertListToDataBytes(List<?> list) {
        final var result = new byte[list.size()];
        for (var index = 0; index < list.size(); index++)
            result[index] = convertToByte(list.get(index));
        return new DataBytes(result);
    }

    protected byte convertToByte(Object object) {
        if (object instanceof Byte value) return value;
        if (object instanceof Short value) return value.byteValue();
        if (object instanceof Integer value) return value.byteValue();
        if (object instanceof Long value) return value.byteValue();
        throw new DataException("Can not convert value to byte: " + object.getClass().getSimpleName());
    }

    protected DataTuple convertTupleToDataTuple(Tuple<?> tuple, TupleType expected) {
        if (tuple == null) return null;
        if (expected == null) expected = inferTupleTypeFromTuple(tuple);
        final var elements = new DataObject[tuple.elements().size()];
        for (var index = 0; index < tuple.elements().size(); index++)
            elements[index] = toDataObject(expected.subType(index), tuple.elements().get(index));
        return new DataTuple(elements);
    }

    protected DataTuple convertListToDataTuple(List<?> list, TupleType expected) {
        if (list == null) return null;
        if (expected == null) expected = inferTupleTypeFromList(list);
        final var elements = new DataObject[list.size()];
        for (var index = 0; index < list.size(); index++)
            elements[index] = toDataObject(expected.subType(index), list.get(index));
        return new DataTuple(elements);
    }

    /**
     * Convert a native {@link List} to a {@link DataList}, converting each element to the provided value type.
     *
     * <p>If an element is not directly assignable to the target value type, a compatibility conversion is attempted
     * via {@link ConvertUtil}.</p>
     *
     * @param list      the source list
     * @param valueType the desired element type for the resulting {@code DataList}
     * @return a {@code DataList} containing converted elements
     */
    protected DataList convertListToDataList(List<?> list, DataType valueType) {
        final var result = new DataList(valueType);
        list.forEach(element -> {
            var dataObject = toDataObject(valueType, element);
            if (valueType.isAssignableFrom(dataObject).isNotAssignable())
                dataObject = converter.convert(null, null, valueType, dataObject, false);
            result.add(toDataObject(valueType, dataObject));
        });
        return result;
    }

    /**
     * Convert a native {@link Map} with String keys to a {@link DataMap} with the given value type.
     *
     * @param map        source map with String keys
     * @param targetType the {@link MapType} describing the desired value type
     * @return a {@link DataMap} containing converted values
     */
    protected DataObject convertMapToDataMap(Map<String, Object> map, MapType targetType) {
        final var result = new DataMap(targetType.valueType());
        map.forEach((key, value) -> result.put(key, toDataObject(targetType.valueType(), value)));
        return result;
    }

    /**
     * Convert a native map to either a {@link DataMap} or a {@link DataStruct}, based on inferred type.
     *
     * <p>The inference is performed by {@link #inferDataTypeFromNativeMap(Map, DataSchema)} using the provided schema
     * hint. When a {@link MapType} is inferred, a {@code DataMap} is produced; when a {@link StructType} is inferred,
     * a {@code DataStruct} is produced.</p>
     *
     * @param map    map with String keys and native values
     * @param schema optional schema hint used for type inference
     * @return a {@link DataObject} representing the converted map
     */
    protected DataObject convertMapToDataStruct(Map<String, Object> map, DataSchema schema) {
        final var type = inferDataTypeFromNativeMap(map, schema);
        if (type instanceof MapType mapType) return convertMapToDataMap(map, mapType);
        if (type instanceof StructType structType) return convertMapToDataStruct(map, structType);
        throw new DataException("Can not convert map to DataObject: " + type.getClass().getSimpleName());
    }

    /**
     * Convert a native map to a {@link DataStruct} using the provided {@link StructType} for field typing.
     *
     * <p>When the struct has a schema, each field's schema is used to determine the target {@link DataType}
     * for the field value. Absent a schema, values are converted using inference.</p>
     *
     * @param map  map with String keys and native values
     * @param type the struct type (with or without schema) to apply
     * @return a {@link DataStruct} containing converted fields
     */
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

    /**
     * Convert a native {@link Tuple} to a {@link DataTuple}, converting each element recursively.
     *
     * @param tuple source tuple with native elements
     * @return a {@code DataTuple} with converted elements
     */
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

        if (value instanceof DataEnum val) return val.value();
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
        return new Struct<>(map.contents(), this::fromDataObject);
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
        final var result = new Struct<>();
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
