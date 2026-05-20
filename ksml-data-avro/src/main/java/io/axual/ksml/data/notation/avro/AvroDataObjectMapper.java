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

import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.mapper.DataObjectMapper;
import io.axual.ksml.data.mapper.DataTypeDataSchemaMapper;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
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
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.data.type.MapType;
import io.axual.ksml.data.type.StructType;
import io.axual.ksml.data.util.ConvertUtil;
import io.axual.ksml.data.value.Struct;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * DataObjectMapper implementation for AVRO native values.
 *
 * <p>Converts between AVRO runtime types (GenericRecord, GenericData.EnumSymbol, GenericFixed,
 * Utf8, byte[]/ByteBuffer, arrays/maps, primitives) and KSML DataObject instances according
 * to the rules in ksml-data/DEVELOPER_GUIDE.md. Reverse mapping produces AVRO-compatible
 * values, optionally using an AVRO Schema derived from a StructSchema when present.</p>
 */
@Slf4j
public class AvroDataObjectMapper implements DataObjectMapper<Object> {
    private static final AvroSchemaMapper SCHEMA_MAPPER = new AvroSchemaMapper();
    private static final NativeDataObjectMapper NATIVE_MAPPER = new NativeDataObjectMapper();
    private static final DataTypeDataSchemaMapper TYPE_SCHEMA_MAPPER = new DataTypeDataSchemaMapper();
    private static final ConvertUtil CONVERTER = new ConvertUtil(NATIVE_MAPPER, TYPE_SCHEMA_MAPPER);

    /**
     * Convert an AVRO-native value into a KSML DataObject.
     *
     * <p>Uses derived method with extra schema parameter for recursion</p>
     *
     * @param expected the expected target DataType used for null handling and numeric coercion
     * @param value    the AVRO-native value to convert (maybe null)
     * @return the corresponding KSML DataObject
     */
    @Override
    public DataObject toDataObject(DataType expected, Object value) {
        return toDataObject(expected, value, null);
    }

    /**
     * Convert an AVRO-native value into a KSML DataObject.
     *
     * <p>Handles Utf8, ByteBuffer, GenericFixed, GenericRecord, EnumSymbol, List and Map,
     * falling back to primitive conversion. Nulls are converted using ConvertUtil based on
     * the expected DataType.</p>
     *
     * @param expected the expected target DataType used for null handling and numeric coercion
     * @param value    the AVRO-native value to convert (maybe null)
     * @param schema   the AVRO schema of the value (maybe null)
     * @return the corresponding KSML DataObject
     */
    private DataObject toDataObject(DataType expected, Object value, Schema schema) {
        // Quick return for NULL values
        if (value == null || value == JsonProperties.NULL_VALUE)
            return ConvertUtil.convertNullToDataObject(expected);

        // Normalize common AVRO wrappers first
        if (value instanceof Utf8 val) value = val.toString();
        if (value instanceof ByteBuffer val) value = toByteArray(val);

        // Convert value based on its type
        return switch (value) {
            case DataObject val -> val;
            case Boolean val -> new DataBoolean(val);
            case Byte val -> expected == DataInteger.DATATYPE ? new DataInteger(val.intValue()) : new DataByte(val);
            case Short val -> expected == DataInteger.DATATYPE ? new DataInteger(val.intValue()) : new DataShort(val);
            case Integer val -> new DataInteger(val);
            case Long val -> new DataLong(val);
            case Double val -> new DataDouble(val);
            case Float val -> new DataFloat(val);
            case byte[] val -> new DataBytes(val);
            case CharSequence val -> new DataString(val.toString());
            case GenericData.EnumSymbol val -> new DataString(val.toString());
            case GenericFixed val -> new DataBytes(val.bytes());
            case GenericRecord val -> convertRecordToDataStruct(expected, val);
            case List<?> val -> convertArrayToDataList(expected, val, schema != null ? elementSchemaOf(schema) : null);
            case Map<?, ?> val -> convertMapToDataMap(expected, val, schema != null ? mapValueSchemaOf(schema) : null);
            default -> throw new DataException("Unsupported primitive type: " + value.getClass().getSimpleName());
        };
    }

    /**
     * Convert a KSML DataObject into an AVRO-compatible value.
     *
     * <p>When a Struct with a StructSchema is provided, an AVRO record is created using a
     * schema derived from that StructSchema. Scalars are converted to their AVRO-native
     * counterparts, with byte and short widened to int to satisfy AVRO's numeric model.</p>
     *
     * @param value the KSML DataObject to convert
     * @return an AVRO-native value (may be null)
     * @throws io.axual.ksml.data.exception.DataException if the value type is unsupported
     */
    @Override
    public Object fromDataObject(DataObject value) {
        return switch (value) {
            case null -> null;
            case DataNull ignored -> null;
            case DataBoolean val -> val.value();
            case DataByte val -> val.value() == null ? null : val.value().intValue();
            case DataShort val -> val.value() == null ? null : val.value().intValue();
            case DataInteger val -> val.value();
            case DataLong val -> val.value();
            case DataDouble val -> val.value();
            case DataFloat val -> val.value();
            case DataString val -> val.value();
            case DataBytes val -> val.value();
            case DataList val -> convertDataListToAvroList(val, null);
            case DataMap val -> convertDataMapToAvroMap(val, null);
            case DataStruct val -> convertDataStructToAvroRecord(val);
            // DataTuple has no real counterpart in AVRO, KSML does not convert Tuples to AVRO
            default ->
                    throw new DataException("Can not convert DataObject to AVRO: " + value.getClass().getSimpleName());
        };
    }

    // ========================= TO DATAOBJECT HELPERS =========================

    private DataObject convertRecordToDataStruct(DataType expected, GenericRecord genericRecord) {
        final var avroSchema = genericRecord.getSchema();
        final var structSchema = (StructSchema) SCHEMA_MAPPER.toDataSchema(avroSchema.getNamespace(), avroSchema.getName(), avroSchema);
        final var result = new DataStruct(structSchema);

        for (var field : avroSchema.getFields()) {
            final var name = field.name();
            final var raw = genericRecord.get(name);

            if (raw != null) {
                // Non-null value: convert based on runtime type and schema
                final var fieldDataSchema = structSchema.field(name) != null ? structSchema.field(name).schema() : null;
                final var fieldExpectedType = TYPE_SCHEMA_MAPPER.fromDataSchema(fieldDataSchema);
                final var conv = toDataObject(fieldExpectedType, raw, field.schema());
                result.put(name, conv);
            } else {
                // Handle optional unions with null defaults based on a concrete branch
                final var nullValue = nullForOptionalField(field.schema());
                // Only add non-null values to the result, i.e., omit for arrays/records/enums -> getter returns null
                if (nullValue != null) result.put(name, nullValue);
            }
        }

        // Make sure the returned DataObject conforms to the expected data type
        return CONVERTER.convert(expected, result);
    }

    private DataObject nullForOptionalField(Schema fieldSchema) {
        final var effective = unwrapUnionToPrimary(fieldSchema);
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
        final var types = schema.getTypes();
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
        // Add elements to the result list
        list.forEach(el -> result.add(toDataObject(elemType, el, elementSchema)));
        // Make sure the returned DataObject conforms to the expected data type
        return CONVERTER.convert(expected, result);
    }

    private DataObject convertMapToDataMap(DataType expected, Map<?, ?> map, Schema valueSchema) {
        final var valType = valueSchema != null ? dataTypeFromAvroSchema(valueSchema) : DataType.UNKNOWN;
        final var result = new DataMap(valType);
        for (final var e : map.entrySet()) {
            final var key = e.getKey() instanceof Utf8 u ? u.toString() : String.valueOf(e.getKey());
            result.put(key, toDataObject(valType, e.getValue(), valueSchema));
        }
        // Make sure the returned DataObject conforms to the expected data type
        return CONVERTER.convert(expected, result);
    }

    private static byte[] toByteArray(ByteBuffer buffer) {
        final var dup = buffer.duplicate();
        final var arr = new byte[dup.remaining()];
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

    @Nullable
    private Map<String, Object> convertDataStructToPlainMap(DataStruct struct) {
        if (struct.isNull()) return null;
        return new Struct<>(struct.contents(), this::fromDataObject);
    }

    // ========================= FROM DATAOBJECT HELPERS =========================

    private Object convertDataObjectToAvroBySchema(DataObject value, Schema schema) {
        if (value == null) return null;
        // TODO: Avro logical types are not validated. KSML treats them as their underlying primitive
        //       and round-trips them without checking the input contract:
        //         * uuid          — any string is accepted, even non-UUID strings
        //         * time-millis   — any int is accepted, even values outside [0, 86_400_000)
        //         * time-micros   — any long is accepted, even values outside [0, 86_400_000_000)
        //         * date          — any int (days since epoch) — accepting any int is technically
        //                           valid by the spec, but no semantic bounds are enforced
        //         * timestamp-*   — same as date — any long passes
        //         * decimal       — bytes/fixed; needs precision/scale-aware BigDecimal encoding/
        //                           decoding (the bytes are big-endian two's-complement integer
        //                           scaled by `scale`). KSML currently exposes the raw bytes only.
        //       Adding first-class handling for these types is a separate feature.
        return switch (schema.getType()) {
            case NULL -> null;
            case BOOLEAN -> value instanceof DataBoolean val ? val.value() : schemaMismatch(value, schema);
            case INT -> switch (value) {
                case DataByte val -> val.value() != null ? val.value().intValue() : null;
                case DataShort val -> val.value() != null ? val.value().intValue() : null;
                case DataInteger val -> val.value();
                case DataLong val -> {
                    if (val.value() == null) yield null;
                    long longVal = val.value();
                    if (longVal < Integer.MIN_VALUE || longVal > Integer.MAX_VALUE) {
                        throw new DataException(("Value %d exceeds INT range [%d, %d]. Use 'long' type in schema "
                                + "or ensure values fit in INT range.")
                                .formatted(longVal, Integer.MIN_VALUE, Integer.MAX_VALUE));
                    }
                    yield (int) longVal;
                }
                case DataFloat val -> {
                    if (val.value() == null) yield null;
                    float floatVal = val.value();
                    if (!Float.isFinite(floatVal) || floatVal < Integer.MIN_VALUE || floatVal > Integer.MAX_VALUE) {
                        throw new DataException(
                                "Value %s cannot be converted to INT (out of range or not finite)".formatted(floatVal));
                    }
                    yield (int) floatVal;
                }
                case DataDouble val -> {
                    if (val.value() == null) yield null;
                    double doubleVal = val.value();
                    if (!Double.isFinite(doubleVal) || doubleVal < Integer.MIN_VALUE || doubleVal > Integer.MAX_VALUE) {
                        throw new DataException(
                                "Value %s cannot be converted to INT (out of range or not finite)".formatted(doubleVal));
                    }
                    yield (int) doubleVal;
                }
                default -> schemaMismatch(value, schema);
            };
            case LONG -> switch (value) {
                case DataByte val -> val.value() != null ? val.value().longValue() : null;
                case DataShort val -> val.value() != null ? val.value().longValue() : null;
                case DataInteger val -> val.value() != null ? val.value().longValue() : null;
                case DataLong val -> val.value();
                case DataFloat val -> {
                    if (val.value() == null) yield null;
                    float floatVal = val.value();
                    if (!Float.isFinite(floatVal) || floatVal >= 0x1.0p63f || floatVal < -0x1.0p63f) {
                        throw new DataException(
                                "Value %s cannot be converted to LONG (out of range or not finite)".formatted(floatVal));
                    }
                    yield (long) floatVal;
                }
                case DataDouble val -> {
                    if (val.value() == null) yield null;
                    double doubleVal = val.value();
                    if (!Double.isFinite(doubleVal) || doubleVal >= 0x1.0p63 || doubleVal < -0x1.0p63) {
                        throw new DataException("Value %s cannot be converted to LONG (out of range or not finite)"
                                .formatted(doubleVal));
                    }
                    yield (long) doubleVal;
                }
                default -> schemaMismatch(value, schema);
            };
            // Accept every numeric DataObject that widens losslessly (or with Java's standard cast
            // semantics) to a double. Without these explicit arms, a perfectly valid pipeline like
            // DataInteger(42) → Avro DOUBLE field would fall through to schemaMismatch and throw —
            // even though the conversion is safe. DataLong → double can lose precision above 2^53
            // (the double mantissa width); we accept that loss explicitly because it matches Java's
            // (double) cast semantics and is the same trade-off other JVM frameworks make.
            case DOUBLE -> switch (value) {
                case DataByte val -> val.value() != null ? val.value().doubleValue() : null;
                case DataShort val -> val.value() != null ? val.value().doubleValue() : null;
                case DataInteger val -> val.value() != null ? val.value().doubleValue() : null;
                case DataLong val -> val.value() != null ? val.value().doubleValue() : null;
                case DataFloat val -> val.value() != null ? val.value().doubleValue() : null;
                case DataDouble val -> val.value();
                default -> schemaMismatch(value, schema);
            };
            // FLOAT accepts every narrower or equal-width numeric source. The DataDouble path is
            // range/finiteness-checked because that is a true narrowing. The DataInteger/DataLong
            // paths can lose bit-exact precision (int values above 2^24, long values above 2^24)
            // but we deliberately do not reject that — see requireFloatRange's Javadoc for the same
            // reasoning applied to DataDouble. If we rejected here, common pipeline values like
            // DataInteger(1_000_000) → FLOAT would fail.
            case FLOAT -> switch (value) {
                case DataByte val -> val.value() != null ? val.value().floatValue() : null;
                case DataShort val -> val.value() != null ? val.value().floatValue() : null;
                case DataInteger val -> val.value() != null ? val.value().floatValue() : null;
                case DataLong val -> val.value() != null ? val.value().floatValue() : null;
                case DataFloat val -> val.value();
                case DataDouble val -> {
                    if (val.value() == null) yield null;
                    double doubleVal = val.value();
                    if (!Double.isFinite(doubleVal) || Math.abs(doubleVal) > Float.MAX_VALUE) {
                        throw new DataException(
                                "Value %s exceeds FLOAT range or is not finite".formatted(doubleVal));
                    }
                    yield (float) doubleVal;
                }
                default -> schemaMismatch(value, schema);
            };
            case BYTES -> value instanceof DataBytes val ? val.value() : schemaMismatch(value, schema);
            case FIXED -> value instanceof DataBytes val ? new GenericData.Fixed(schema, val.value()) : schemaMismatch(value, schema);
            case STRING -> value instanceof DataString val ? val.value() : schemaMismatch(value, schema);
            case ENUM -> {
                // Validate two things up front: (1) the value is a DataString or DataEnum (anything
                // else previously yielded null and failed downstream with a generic "value cannot be
                // null" error), and (2) the symbol is actually declared in the schema (Avro's
                // GenericData.EnumSymbol does not validate that itself).
                final String symbol;
                if (value instanceof DataString s) {
                    symbol = s.value();
                } else if (value instanceof DataEnum e) {
                    symbol = e.value();
                } else {
                    throw new DataException("Can not convert " + value.getClass().getSimpleName()
                            + " to Avro ENUM '" + schema.getFullName() + "'");
                }
                if (symbol == null) yield null;
                if (!schema.hasEnumSymbol(symbol)) {
                    throw new DataException("Value \"" + symbol + "\" is not a valid symbol of Avro ENUM '"
                            + schema.getFullName() + "' (allowed: " + schema.getEnumSymbols() + ")");
                }
                yield new GenericData.EnumSymbol(schema, symbol);
            }
            case ARRAY -> value instanceof DataList val
                    ? convertDataListToAvroList(val, schema.getElementType())
                    : schemaMismatch(value, schema);
            case MAP -> value instanceof DataMap val
                    ? convertDataMapToAvroMap(val, schema.getValueType())
                    : schemaMismatch(value, schema);
            case RECORD -> value instanceof DataStruct val
                    ? convertDataStructToAvroRecord(val)
                    : schemaMismatch(value, schema);
            case UNION -> {
                if (value instanceof DataNull) yield null;
                // Pick the union branch that natively matches the DataObject type first; only fall back
                // to attempting a conversion if no branch is a native match. The previous behaviour was
                // "first non-null candidate wins", which routed e.g. DataLong(5) into a [DOUBLE,LONG]
                // union as Long-into-DOUBLE — Avro then failed later with a confusing class-cast error.
                final var nativeMatch = matchUnionBranchByNativeType(schema, value);
                if (nativeMatch != null) yield convertDataObjectToAvroBySchema(value, nativeMatch);
                for (var branch : schema.getTypes()) {
                    if (branch.getType() == Schema.Type.NULL) continue;
                    var candidate = convertDataObjectToAvroBySchema(value, branch);
                    if (candidate != null) yield candidate;
                }
                throw new DataException("No union branch in '" + schema + "' is compatible with "
                        + value.getClass().getSimpleName());
            }
        };
    }

    /**
     * Handles a mismatch between a {@link DataObject} and the target Avro {@link Schema} during
     * serialization.
     *
     * <p>Replaces the previous {@code default → fromDataObject(value)} fallback arms which silently
     * produced a native Java value that did not match the Avro field type (e.g. a {@link DataString}
     * sent to an INT field passed through as a Java {@code String} and then failed deep inside Avro
     * with a generic class-cast error). {@link DataNull} is preserved as {@code null} so nullable
     * fields keep working.</p>
     *
     * @param value  the offending DataObject
     * @param schema the target Avro schema
     * @return {@code null} if {@code value} is a {@link DataNull}
     * @throws DataException for any other DataObject type
     */
    private Object schemaMismatch(DataObject value, Schema schema) {
        if (value instanceof DataNull) return null;
        throw new DataException("Cannot convert " + value.getClass().getSimpleName()
                + " to Avro " + schema.getType() + " field");
    }

    /**
     * Picks the union branch whose Avro type natively matches the runtime {@link DataObject} type.
     *
     * <p>Used to disambiguate unions before falling back to attempting value coercion through every
     * branch. Without this step, the previous "first non-null candidate wins" behaviour routed e.g.
     * a {@link DataLong} into a {@code [DOUBLE, LONG]} union as a Long-in-the-DOUBLE-slot, which
     * Avro then rejected later with a confusing class-cast error.</p>
     *
     * @param unionSchema the Avro UNION schema being targeted
     * @param value       the runtime DataObject to place
     * @return the matching branch schema, or {@code null} when no branch has a native type match
     */
    private Schema matchUnionBranchByNativeType(Schema unionSchema, DataObject value) {
        final var preferred = switch (value) {
            case DataBoolean ignored -> Schema.Type.BOOLEAN;
            case DataByte ignored -> Schema.Type.INT;
            case DataShort ignored -> Schema.Type.INT;
            case DataInteger ignored -> Schema.Type.INT;
            case DataLong ignored -> Schema.Type.LONG;
            case DataFloat ignored -> Schema.Type.FLOAT;
            case DataDouble ignored -> Schema.Type.DOUBLE;
            case DataString ignored -> Schema.Type.STRING;
            case DataBytes ignored -> Schema.Type.BYTES;
            case DataList ignored -> Schema.Type.ARRAY;
            case DataMap ignored -> Schema.Type.MAP;
            case DataStruct ignored -> Schema.Type.RECORD;
            case DataEnum ignored -> Schema.Type.ENUM;
            default -> null;
        };
        if (preferred == null) return null;
        for (var branch : unionSchema.getTypes()) {
            if (branch.getType() == preferred) return branch;
        }
        return null;
    }

    @Nullable
    private List<Object> convertDataListToAvroList(DataList list, Schema elementSchema) {
        if (list.isNull()) return null;
        List<Object> result = new ArrayList<>(list.size());
        list.forEach(element -> result.add(elementSchema != null ? convertDataObjectToAvroBySchema(element, elementSchema) : fromDataObject(element)));
        return result;
    }

    @Nullable
    private Map<String, Object> convertDataMapToAvroMap(DataMap map, Schema valueSchema) {
        if (map.isNull()) return null;
        return new Struct<>(
                map.contents(),
                v -> valueSchema != null ? convertDataObjectToAvroBySchema(v, valueSchema) : fromDataObject(v));
    }

    private Object convertDataStructToAvroRecord(DataStruct struct) {
        if (struct.isNull()) return null;

        // Build AVRO schema from the given struct type if available
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
}
