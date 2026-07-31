package io.axual.ksml.data.notation.avro;

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

import io.axual.ksml.data.schema.logical.DecimalLogicalType;
import io.axual.ksml.data.schema.logical.LogicalType;
import io.axual.ksml.data.schema.logical.LogicalTypeRegistry;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;

/** Adapter between Avro's logical types and the notation-neutral {@link LogicalType} model. */
final class AvroLogicalTypes {
    private AvroLogicalTypes() {
    }

    /** Resolves the logical type of a concrete (non-union) Avro schema, or null when there is none or it is unknown. */
    static LogicalType resolve(Schema schema) {
        if (schema == null) return null;
        final var avroLogical = schema.getLogicalType();
        if (avroLogical == null) return null;
        if (avroLogical instanceof LogicalTypes.Decimal decimal) {
            // Bytes-backed decimal only for now; fixed-backed decimal falls back to a plain fixed.
            if (schema.getType() != Schema.Type.BYTES) return null;
            return new DecimalLogicalType(decimal.getPrecision(), decimal.getScale());
        }
        return LogicalTypeRegistry.byName(avroLogical.getName());
    }

    /** Like {@link #resolve(Schema)} but looks through an optional union to its non-null branch. */
    static LogicalType resolveEffective(Schema schema) {
        if (schema == null) return null;
        if (schema.getType() == Schema.Type.UNION) {
            for (final var branch : schema.getTypes()) {
                if (branch.getType() == Schema.Type.NULL) continue;
                final var logicalType = resolve(branch);
                if (logicalType != null) return logicalType;
            }
            return null;
        }
        return resolve(schema);
    }

    /** Attaches the Avro logical type matching the given {@link LogicalType} to a base Avro schema. */
    static Schema apply(Schema base, LogicalType logicalType) {
        return switch (logicalType.name()) {
            case DecimalLogicalType.LOGICAL_TYPE_NAME -> {
                final var decimal = (DecimalLogicalType) logicalType;
                yield LogicalTypes.decimal(decimal.precision(), decimal.scale()).addToSchema(base);
            }
            case "uuid" -> LogicalTypes.uuid().addToSchema(base);
            case "date" -> LogicalTypes.date().addToSchema(base);
            case "time-millis" -> LogicalTypes.timeMillis().addToSchema(base);
            case "time-micros" -> LogicalTypes.timeMicros().addToSchema(base);
            case "timestamp-millis" -> LogicalTypes.timestampMillis().addToSchema(base);
            case "timestamp-micros" -> LogicalTypes.timestampMicros().addToSchema(base);
            case "local-timestamp-millis" -> LogicalTypes.localTimestampMillis().addToSchema(base);
            case "local-timestamp-micros" -> LogicalTypes.localTimestampMicros().addToSchema(base);
            default -> base;
        };
    }

    static String decimalToString(byte[] unscaledTwosComplement, int scale) {
        return new BigDecimal(new BigInteger(unscaledTwosComplement), scale).toPlainString();
    }

    static ByteBuffer stringToDecimalBytes(String value, int scale) {
        final var scaled = new BigDecimal(value).setScale(scale, RoundingMode.UNNECESSARY);
        return ByteBuffer.wrap(scaled.unscaledValue().toByteArray());
    }
}
