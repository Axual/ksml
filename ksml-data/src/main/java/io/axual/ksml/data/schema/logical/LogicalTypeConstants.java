package io.axual.ksml.data.schema.logical;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library
 * %%
 * Copyright (C) 2021 - 2026 Axual B.V.
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
import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataLong;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.type.DataType;

import java.util.Map;
import java.util.function.Consumer;

/**
 * The canonical logical-type names and the standard scalar logical types that carry them.
 *
 * <p>Decimal is not here because it is parameterised by precision and scale; see
 * {@link DecimalLogicalType}. An unknown or vendor-specific name resolves to null, matching Avro's
 * "ignore it and use the base type" rule.</p>
 */
public final class LogicalTypeConstants {
    private static final long MILLIS_PER_DAY = 86_400_000L;
    private static final long MICROS_PER_DAY = 86_400_000_000L;

    // Names
    public static final String UUID = "uuid";
    public static final String DECIMAL = "decimal";
    public static final String DATE = "date";
    public static final String TIME_MILLIS = "time-millis";
    public static final String TIME_MICROS = "time-micros";
    public static final String TIMESTAMP_MILLIS = "timestamp-millis";
    public static final String TIMESTAMP_MICROS = "timestamp-micros";
    public static final String LOCAL_TIMESTAMP_MILLIS = "local-timestamp-millis";
    public static final String LOCAL_TIMESTAMP_MICROS = "local-timestamp-micros";

    // Shared instances
    public static final LogicalType UUID_TYPE = simple(UUID, DataSchema.STRING_SCHEMA, DataString.DATATYPE, LogicalTypeConstants::validateUuid);
    public static final LogicalType DATE_TYPE = simple(DATE, DataSchema.INTEGER_SCHEMA, DataInteger.DATATYPE, LogicalTypeConstants::noValidation);
    public static final LogicalType TIME_MILLIS_TYPE = simple(TIME_MILLIS, DataSchema.INTEGER_SCHEMA, DataInteger.DATATYPE, LogicalTypeConstants::validateTimeMillis);
    public static final LogicalType TIME_MICROS_TYPE = simple(TIME_MICROS, DataSchema.LONG_SCHEMA, DataLong.DATATYPE, LogicalTypeConstants::validateTimeMicros);
    public static final LogicalType TIMESTAMP_MILLIS_TYPE = simple(TIMESTAMP_MILLIS, DataSchema.LONG_SCHEMA, DataLong.DATATYPE, LogicalTypeConstants::noValidation);
    public static final LogicalType TIMESTAMP_MICROS_TYPE = simple(TIMESTAMP_MICROS, DataSchema.LONG_SCHEMA, DataLong.DATATYPE, LogicalTypeConstants::noValidation);
    public static final LogicalType LOCAL_TIMESTAMP_MILLIS_TYPE = simple(LOCAL_TIMESTAMP_MILLIS, DataSchema.LONG_SCHEMA, DataLong.DATATYPE, LogicalTypeConstants::noValidation);
    public static final LogicalType LOCAL_TIMESTAMP_MICROS_TYPE = simple(LOCAL_TIMESTAMP_MICROS, DataSchema.LONG_SCHEMA, DataLong.DATATYPE, LogicalTypeConstants::noValidation);

    private static final Map<String, LogicalType> BY_NAME = Map.of(
            UUID, UUID_TYPE,
            DATE, DATE_TYPE,
            TIME_MILLIS, TIME_MILLIS_TYPE,
            TIME_MICROS, TIME_MICROS_TYPE,
            TIMESTAMP_MILLIS, TIMESTAMP_MILLIS_TYPE,
            TIMESTAMP_MICROS, TIMESTAMP_MICROS_TYPE,
            LOCAL_TIMESTAMP_MILLIS, LOCAL_TIMESTAMP_MILLIS_TYPE,
            LOCAL_TIMESTAMP_MICROS, LOCAL_TIMESTAMP_MICROS_TYPE);

    private LogicalTypeConstants() {
    }

    /** Resolves a standard scalar logical type by name, or null for an unknown or custom name. */
    public static LogicalType byName(String name) {
        return name == null ? null : BY_NAME.get(name);
    }

    private static LogicalType simple(String name, DataSchema baseSchema, DataType representationType, Consumer<DataObject> validator) {
        return new SimpleLogicalType(name, baseSchema, representationType, validator);
    }

    private static void noValidation(DataObject value) {
        // Nothing to check: any value of the base type is valid.
    }

    private static void validateUuid(DataObject value) {
        if (!(value instanceof DataString stringValue) || stringValue.value() == null) return;
        try {
            // Fully qualified because this class also declares a UUID name constant.
            java.util.UUID.fromString(stringValue.value());
        } catch (IllegalArgumentException e) {
            throw new DataException("Value \"" + stringValue.value() + "\" is not a valid uuid", e);
        }
    }

    private static void validateTimeMillis(DataObject value) {
        requireRangeIfNumber(value, 0, MILLIS_PER_DAY - 1, TIME_MILLIS);
    }

    private static void validateTimeMicros(DataObject value) {
        requireRangeIfNumber(value, 0, MICROS_PER_DAY - 1, TIME_MICROS);
    }

    /**
     * Range-checks any integral value, whatever DataObject class carries it. Checking the number rather
     * than the wrapper class matters because a Python integer does not always arrive as the exact class
     * of the base type. A non-integral value is left to the notation mapper to reject.
     */
    private static void requireRangeIfNumber(DataObject value, long min, long max, String logicalName) {
        final Long number = switch (value) {
            case DataInteger val -> val.value() == null ? null : val.value().longValue();
            case DataLong val -> val.value();
            case null, default -> null;
        };
        if (number == null) return;
        if (number < min || number > max)
            throw new DataException("Value " + number + " is out of range for " + logicalName + " [" + min + ", " + max + "]");
    }

    private record SimpleLogicalType(String name, DataSchema baseSchema, DataType representationType,
                                     Consumer<DataObject> validator) implements LogicalType {
        @Override
        public DataSchema representationSchema() {
            return baseSchema;
        }

        @Override
        public void validate(DataObject value) {
            validator.accept(value);
        }

        @Override
        public String toString() {
            return name;
        }
    }
}
