package io.axual.ksml.data.notation.avro;

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

import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.mapper.DataObjectMapper;
import io.axual.ksml.data.mapper.DataTypeDataSchemaMapper;
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
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.data.type.MapType;
import io.axual.ksml.data.type.StructType;
import io.axual.ksml.data.util.ConvertUtil;
import lombok.extern.slf4j.Slf4j;

/**
 * DataObjectMapper implementation for Avro native values.
 *
 * <p>Converts between Avro runtime types (GenericRecord, GenericData.EnumSymbol, GenericFixed,
 * Utf8, byte[]/ByteBuffer, arrays/maps, primitives) and KSML DataObject instances according
 * to the rules in ksml-data/DEVELOPER_GUIDE.md. Reverse mapping produces Avro-compatible
 * values, optionally using an Avro Schema derived from a StructSchema when present.</p>
 */
@Slf4j
public class AvroDataObjectMapper implements DataObjectMapper<Object> {
    private static final AvroSchemaMapper SCHEMA_MAPPER = new AvroSchemaMapper();
    private static final DataTypeDataSchemaMapper TYPE_SCHEMA_MAPPER = new DataTypeDataSchemaMapper();

    /**
     * Convert an Avro-native value into a KSML DataObject.
     *
     * <p>Handles Utf8, ByteBuffer, GenericFixed, GenericRecord, EnumSymbol, List and Map,
     * falling back to primitive conversion. Nulls are converted using ConvertUtil based on
     * the expected DataType.</p>
     *
     * @param expected the expected target DataType used for null handling and numeric coercion
     * @param value    the Avro-native value to convert (may be null)
     * @return the corresponding KSML DataObject
     */
    @Override
    public DataObject toDataObject(DataType expected, Object value) {
        if (value == null) return ConvertUtil.convertNullToDataObject(expected);

        // Normalize common Avro wrappers first
        if (value instanceof Utf8 utf8) value = utf8.toString();
        if (value instanceof ByteBuffer bb) value = toByteArray(bb);

        if (value instanceof GenericData.EnumSymbol enumSym) {
            return new DataString(enumSym.toString());
        }
        if (value instanceof GenericFixed fixed) {
            return new DataBytes(fixed.bytes());
        }
        if (value instanceof GenericRecord genericRecord) {
            return convertRecordToDataStruct(expected, genericRecord);
        }
        if (value instanceof List<?> list) {
            return convertArrayToDataList(expected, list, null);
        }
        if (value instanceof Map<?, ?> map) {
            return convertMapToDataMap(expected, map, null);
        }

        // Fallback: primitives and simple types
        return primitiveToDataObject(expected, value);
    }

    /**
     * Convert a KSML DataObject into an Avro-compatible value.
     *
     * <p>When a Struct with a StructSchema is provided, an Avro record is created using a
     * schema derived from that StructSchema. Scalars are converted to their Avro-native
     * counterparts, with byte and short widened to int to satisfy Avro's numeric model.</p>
     *
     * @param value the KSML DataObject to convert
     * @return an Avro-native value (may be null)
     * @throws io.axual.ksml.data.exception.DataException if the value type is unsupported
     */
    @Override
    public Object fromDataObject(DataObject value) {
        return switch (value) {
            case null -> null;
            case DataNull ignored -> null;
            case DataBoolean v -> v.value();
            case DataByte v -> v.value() == null ? null : v.value().intValue();
            case DataShort v -> v.value() == null ? null : v.value().intValue();
            case DataInteger v -> v.value();
            case DataLong v -> v.value();
            case DataFloat v -> v.value();
            case DataDouble v -> v.value();
            case DataString v -> v.value();
            case DataBytes v -> v.value();
            case DataList v -> convertDataListToAvroList(v, null);
            case DataMap v -> convertDataMapToAvroMap(v, null);
            case DataStruct v -> convertDataStructToAvroRecord(v);
            // DataTuple has no real counterpart in Avro, KSML does not support Avro with Tuples right
            default ->
                    throw new DataException("Can not convert DataObject to AVRO: " + value.getClass().getSimpleName());
        };
    }

    // ========================= TO DATAOBJECT HELPERS =========================

    private DataObject convertRecordToDataStruct(DataType expected, GenericRecord genericRecord) {
        var avroSchema = genericRecord.getSchema();
        var structSchema = (StructSchema) SCHEMA_MAPPER.toDataSchema(avroSchema.getNamespace(), avroSchema.getName(), avroSchema);
        var result = new DataStruct(structSchema);

        for (var field : avroSchema.getFields()) {
            final var name = field.name();
            final var raw = genericRecord.get(name);
            final var fieldDataSchema = structSchema.field(name) != null ? structSchema.field(name).schema() : null;
            final var fieldExpectedType = TYPE_SCHEMA_MAPPER.fromDataSchema(fieldDataSchema);

            if (raw == null) {
                // Handle optional unions with null defaults based on concrete branch
                var nullValue = nullForOptionalField(field.schema());
                if (nullValue != null) {
                    result.put(name, nullValue);
                }
                // else: omit field for arrays/records/enums -> getter returns null
                continue;
            }

            // Non-null value: convert based on runtime type and schema
            var conv = convertAvroValueToDataObject(fieldExpectedType, raw, field.schema());
            result.put(name, conv);
        }
        return result;
    }

    private DataObject convertAvroValueToDataObject(DataType expected, Object value, Schema avroFieldSchema) {
        if (value == null) return ConvertUtil.convertNullToDataObject(expected);
        if (value instanceof Utf8 utf8) value = utf8.toString();
        if (value instanceof ByteBuffer bb) value = toByteArray(bb);
        if (value instanceof GenericData.EnumSymbol enumSym)
            return new DataString(enumSym.toString());
        if (value instanceof GenericFixed fixed) return new DataBytes(fixed.bytes());
        if (value instanceof GenericRecord rec) return convertRecordToDataStruct(expected, rec);
        if (value instanceof List<?> list)
            return convertArrayToDataList(expected, list, elementSchemaOf(avroFieldSchema));
        if (value instanceof Map<?, ?> map)
            return convertMapToDataMap(expected, map, mapValueSchemaOf(avroFieldSchema));
        return primitiveToDataObject(expected, value);
    }

    private DataObject nullForOptionalField(Schema fieldSchema) {
        var effective = unwrapUnionToPrimary(fieldSchema);
        if (effective == null) return null; // not an optional union or ambiguous union
        return switch (effective.getType()) {
            case STRING -> new DataString(null);
            case INT -> new DataInteger(null);
            case LONG -> new DataLong(null);
            case FLOAT -> new DataFloat(null);
            case DOUBLE -> new DataDouble(null);
            case BOOLEAN -> new DataBoolean(null);
            case BYTES -> new DataBytes(null);
            case MAP -> {
                var valueSchema = effective.getValueType();
                var valueDataSchema = SCHEMA_MAPPER.toDataSchema(null, valueSchema);
                var valueType = TYPE_SCHEMA_MAPPER.fromDataSchema(valueDataSchema);
                yield new DataMap(valueType, true);
            }
            // For ARRAY, RECORD, ENUM, FIXED, MAP -> omit field (return null)
            default -> null;
        };
    }

    private Schema unwrapUnionToPrimary(Schema schema) {
        if (schema.getType() != Schema.Type.UNION) return null;
        var types = schema.getTypes();
        // Optional pattern: [null, T] or [T, null]
        if (types.size() == 2) {
            if (types.get(0).getType() == Schema.Type.NULL) return types.get(1);
            if (types.get(1).getType() == Schema.Type.NULL) return types.get(0);
        }
        return null; // other unions not treated as simple optional
    }

    private Schema elementSchemaOf(Schema fieldSchema) {
        var s = fieldSchema.getType() == Schema.Type.UNION ? activeNonNullArraySchema(fieldSchema) : fieldSchema;
        if (s != null && s.getType() == Schema.Type.ARRAY) return s.getElementType();
        return null;
    }

    private Schema activeNonNullArraySchema(Schema unionSchema) {
        for (var s : unionSchema.getTypes()) if (s.getType() == Schema.Type.ARRAY) return s;
        return null;
    }

    private Schema mapValueSchemaOf(Schema fieldSchema) {
        var s = fieldSchema.getType() == Schema.Type.UNION ? activeNonNullMapSchema(fieldSchema) : fieldSchema;
        if (s != null && s.getType() == Schema.Type.MAP) return s.getValueType();
        return null;
    }

    private Schema activeNonNullMapSchema(Schema unionSchema) {
        for (var s : unionSchema.getTypes()) if (s.getType() == Schema.Type.MAP) return s;
        return null;
    }

    private DataObject convertArrayToDataList(DataType expected, List<?> list, Schema elementSchema) {
        var elemType = elementSchema != null ? dataTypeFromAvroSchema(elementSchema) : DataType.UNKNOWN;
        var result = new DataList(elemType);
        for (var el : list) {
            result.add(toDataObject(elemType, el));
        }
        return result;
    }

    private DataObject convertMapToDataMap(DataType expected, Map<?, ?> map, Schema valueSchema) {
        var valType = valueSchema != null ? dataTypeFromAvroSchema(valueSchema) : DataType.UNKNOWN;
        var result = new DataMap(valType);
        for (var e : map.entrySet()) {
            var key = e.getKey() instanceof Utf8 u ? u.toString() : String.valueOf(e.getKey());
            result.put(key, toDataObject(valType, e.getValue()));
        }
        return result;
    }

    private static byte[] toByteArray(ByteBuffer buffer) {
        var dup = buffer.duplicate();
        var arr = new byte[dup.remaining()];
        dup.get(arr);
        return arr;
    }

    private DataType dataTypeFromAvroSchema(Schema schema) {
        if (schema == null) return DataType.UNKNOWN;
        return switch (schema.getType()) {
            case NULL -> DataNull.DATATYPE;
            case BOOLEAN -> DataBoolean.DATATYPE;
            case INT -> DataInteger.DATATYPE;
            case LONG -> DataLong.DATATYPE;
            case FLOAT -> DataFloat.DATATYPE;
            case DOUBLE -> DataDouble.DATATYPE;
            case BYTES, FIXED -> DataBytes.DATATYPE;
            case STRING, ENUM -> DataString.DATATYPE;
            case ARRAY -> new ListType(dataTypeFromAvroSchema(schema.getElementType()));
            case MAP -> new MapType(dataTypeFromAvroSchema(schema.getValueType()));
            case RECORD ->
                    new StructType((StructSchema) SCHEMA_MAPPER.toDataSchema(schema.getNamespace(), schema.getName(), schema));
            case UNION -> {
                // Heuristic: if union is [null, T] return T; otherwise unknown
                var types = schema.getTypes();
                if (types.size() == 2) {
                    var primary = unwrapUnionToPrimary(schema);
                    yield primary != null ? dataTypeFromAvroSchema(primary) : DataType.UNKNOWN;
                }
                yield DataType.UNKNOWN;
            }
        };
    }

    private DataObject primitiveToDataObject(DataType expected, Object value) {
        if (value == null || value == JsonProperties.NULL_VALUE)
            return ConvertUtil.convertNullToDataObject(expected);
        if (value instanceof DataObject d) return d;
        if (value instanceof Boolean v) return new DataBoolean(v);
        if (value instanceof Byte v)
            return expected == DataInteger.DATATYPE ? new DataInteger(v.intValue()) : new DataByte(v);
        if (value instanceof Short v)
            return expected == DataInteger.DATATYPE ? new DataInteger(v.intValue()) : new DataShort(v);
        if (value instanceof Integer v) return new DataInteger(v);
        if (value instanceof Long v) return new DataLong(v);
        if (value instanceof Float v) return new DataFloat(v);
        if (value instanceof Double v) return new DataDouble(v);
        if (value instanceof byte[] v) return new DataBytes(v);
        if (value instanceof CharSequence v) return new DataString(v.toString());
        throw new DataException("Unsupported primitive type: " + value.getClass().getSimpleName());
    }

    private Map<String, Object> convertDataStructToPlainMap(DataStruct struct) {
        if (struct.isNull()) return null;
        Map<String, Object> out = new TreeMap<>(DataStruct.COMPARATOR);
        struct.forEach((k, v) -> out.put(k, fromDataObject(v)));
        return out;
    }

    // ========================= FROM DATAOBJECT HELPERS =========================

    private Object convertDataStructToAvroRecord(DataStruct struct) {
        if (struct.isNull()) return null;

        // Build Avro schema from struct type if available
        var ksmlSchema = struct.type() != null ? struct.type().schema() : null;
        var avroSchema = ksmlSchema != null ? SCHEMA_MAPPER.fromDataSchema(ksmlSchema) : null;
        if (avroSchema == null || avroSchema.getType() != Schema.Type.RECORD) {
            // Fallback to native map conversion if no schema
            return convertDataStructToPlainMap(struct);
        }
        var rec = new GenericData.Record(avroSchema);
        for (var f : avroSchema.getFields()) {
            var fieldVal = struct.get(f.name());
            var avroVal = convertDataObjectToAvroBySchema(fieldVal, f.schema());
            rec.put(f.name(), avroVal);
        }
        return rec;
    }

    private Object convertDataObjectToAvroBySchema(DataObject value, Schema schema) {
        if (value == null) return null;
        switch (schema.getType()) {
            case STRING -> {
                if (value instanceof DataString s) return s.value();
                return fromDataObject(value);
            }
            case INT -> {
                if (value instanceof DataInteger v) return v.value();
                if (value instanceof DataByte v)
                    return v.value() != null ? v.value().intValue() : null;
                if (value instanceof DataShort v)
                    return v.value() != null ? v.value().intValue() : null;
                return fromDataObject(value);
            }
            case LONG -> {
                if (value instanceof DataLong v) return v.value();
                return fromDataObject(value);
            }
            case FLOAT -> {
                if (value instanceof DataFloat v) return v.value();
                return fromDataObject(value);
            }
            case DOUBLE -> {
                if (value instanceof DataDouble v) return v.value();
                return fromDataObject(value);
            }
            case BOOLEAN -> {
                if (value instanceof DataBoolean v) return v.value();
                return fromDataObject(value);
            }
            case BYTES -> {
                if (value instanceof DataBytes v) return v.value();
                return fromDataObject(value);
            }
            case FIXED -> {
                if (value instanceof DataBytes v) return new GenericData.Fixed(schema, v.value());
                return null;
            }
            case ENUM -> {
                if (value instanceof DataString s)
                    return new GenericData.EnumSymbol(schema, s.value());
                return null;
            }
            case ARRAY -> {
                if (value instanceof DataList list)
                    return convertDataListToAvroList(list, schema.getElementType());
                return null;
            }
            case MAP -> {
                if (value instanceof DataMap map)
                    return convertDataMapToAvroMap(map, schema.getValueType());
                return null;
            }
            case RECORD -> {
                if (value instanceof DataStruct st) return convertDataStructToAvroRecord(st);
                return null;
            }
            case UNION -> {
                if (value instanceof DataNull) return null;
                // Try to match one of the union branches
                for (var branch : schema.getTypes()) {
                    if (branch.getType() == Schema.Type.NULL) continue;
                    var candidate = convertDataObjectToAvroBySchema(value, branch);
                    if (candidate != null || value instanceof DataNull) return candidate;
                }
                // Last resort
                return fromDataObject(value);
            }
            default -> {
                return fromDataObject(value);
            }
        }
    }

    private List<Object> convertDataListToAvroList(DataList list, Schema elementSchema) {
        if (list.isNull()) return null;
        List<Object> out = new ArrayList<>(list.size());
        for (var el : list) {
            out.add(elementSchema != null ? convertDataObjectToAvroBySchema(el, elementSchema) : fromDataObject(el));
        }
        return out;
    }

    private Map<String, Object> convertDataMapToAvroMap(DataMap map, Schema valueSchema) {
        if (map.isNull()) return null;
        Map<String, Object> out = new TreeMap<>();
        map.forEach((k, v) -> out.put(k, valueSchema != null ? convertDataObjectToAvroBySchema(v, valueSchema) : fromDataObject(v)));
        return out;
    }
}
