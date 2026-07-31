package io.axual.ksml.data.schema.logical;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2025 Axual B.V.
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
 * The standard scalar logical types with fixed parameters (everything except parameterised decimal).
 * An unknown or vendor-specific name resolves to null, matching Avro's "ignore and use the base type" rule.
 */
public final class LogicalTypeRegistry {
    private static final long MILLIS_PER_DAY = 86_400_000L;
    private static final long MICROS_PER_DAY = 86_400_000_000L;

    public static final LogicalType UUID = simple("uuid", DataSchema.STRING_SCHEMA, DataString.DATATYPE, LogicalTypeRegistry::validateUuid);
    public static final LogicalType DATE = simple("date", DataSchema.INTEGER_SCHEMA, DataInteger.DATATYPE, LogicalTypeRegistry::noValidation);
    public static final LogicalType TIME_MILLIS = simple("time-millis", DataSchema.INTEGER_SCHEMA, DataInteger.DATATYPE, LogicalTypeRegistry::validateTimeMillis);
    public static final LogicalType TIME_MICROS = simple("time-micros", DataSchema.LONG_SCHEMA, DataLong.DATATYPE, LogicalTypeRegistry::validateTimeMicros);
    public static final LogicalType TIMESTAMP_MILLIS = simple("timestamp-millis", DataSchema.LONG_SCHEMA, DataLong.DATATYPE, LogicalTypeRegistry::noValidation);
    public static final LogicalType TIMESTAMP_MICROS = simple("timestamp-micros", DataSchema.LONG_SCHEMA, DataLong.DATATYPE, LogicalTypeRegistry::noValidation);
    public static final LogicalType LOCAL_TIMESTAMP_MILLIS = simple("local-timestamp-millis", DataSchema.LONG_SCHEMA, DataLong.DATATYPE, LogicalTypeRegistry::noValidation);
    public static final LogicalType LOCAL_TIMESTAMP_MICROS = simple("local-timestamp-micros", DataSchema.LONG_SCHEMA, DataLong.DATATYPE, LogicalTypeRegistry::noValidation);

    private static final Map<String, LogicalType> BY_NAME = Map.of(
            UUID.name(), UUID,
            DATE.name(), DATE,
            TIME_MILLIS.name(), TIME_MILLIS,
            TIME_MICROS.name(), TIME_MICROS,
            TIMESTAMP_MILLIS.name(), TIMESTAMP_MILLIS,
            TIMESTAMP_MICROS.name(), TIMESTAMP_MICROS,
            LOCAL_TIMESTAMP_MILLIS.name(), LOCAL_TIMESTAMP_MILLIS,
            LOCAL_TIMESTAMP_MICROS.name(), LOCAL_TIMESTAMP_MICROS);

    private LogicalTypeRegistry() {
    }

    /** Resolves a standard scalar logical type by name, or null for an unknown or custom name. */
    public static LogicalType byName(String name) {
        return name == null ? null : BY_NAME.get(name);
    }

    private static LogicalType simple(String name, DataSchema baseSchema, DataType representationType, Consumer<DataObject> validator) {
        return new SimpleLogicalType(name, baseSchema, representationType, validator);
    }

    private static void noValidation(DataObject value) {
        // No constraints to enforce: every value of the base type is a valid instance of this logical type.
    }

    private static void validateUuid(DataObject value) {
        if (!(value instanceof DataString stringValue) || stringValue.value() == null) return;
        try {
            java.util.UUID.fromString(stringValue.value());
        } catch (IllegalArgumentException _) {
            throw new DataException("Value \"" + stringValue.value() + "\" is not a valid uuid");
        }
    }

    private static void validateTimeMillis(DataObject value) {
        if (!(value instanceof DataInteger intValue) || intValue.value() == null) return;
        requireRange(intValue.value(), 0, MILLIS_PER_DAY - 1, "time-millis");
    }

    private static void validateTimeMicros(DataObject value) {
        if (!(value instanceof DataLong longValue) || longValue.value() == null) return;
        requireRange(longValue.value(), 0, MICROS_PER_DAY - 1, "time-micros");
    }

    private static void requireRange(long value, long min, long max, String logicalName) {
        if (value < min || value > max)
            throw new DataException("Value " + value + " is out of range for " + logicalName + " [" + min + ", " + max + "]");
    }

    private record SimpleLogicalType(String name, DataSchema baseSchema, DataType representationType, Consumer<DataObject> validator) implements LogicalType {
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
