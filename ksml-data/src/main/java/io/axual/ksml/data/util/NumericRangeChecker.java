package io.axual.ksml.data.util;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library
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
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Internal range-check helpers for narrowing numeric conversions.
 *
 * <p>Used by {@code NativeDataObjectMapper} (native Java → DataObject) and {@code ConvertUtil}
 * (DataObject → DataObject). Both perform the same set of narrowing casts and need the same
 * pre-cast validation.</p>
 *
 * <p><b>Internal API.</b> The methods are {@code public} only because the two call sites live in
 * different packages. Do not call from outside the {@code ksml-data} numeric conversion paths.</p>
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class NumericRangeChecker {
    public static void requireByteRange(long value) {
        if (value < Byte.MIN_VALUE || value > Byte.MAX_VALUE) {
            throw new DataException(
                    "Value %d exceeds BYTE range [%d, %d]".formatted(value, Byte.MIN_VALUE, Byte.MAX_VALUE));
        }
    }

    public static void requireShortRange(long value) {
        if (value < Short.MIN_VALUE || value > Short.MAX_VALUE) {
            throw new DataException(
                    "Value %d exceeds SHORT range [%d, %d]".formatted(value, Short.MIN_VALUE, Short.MAX_VALUE));
        }
    }

    public static void requireIntRange(long value) {
        if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
            throw new DataException(
                    "Value %d exceeds INT range [%d, %d]".formatted(value, Integer.MIN_VALUE, Integer.MAX_VALUE));
        }
    }

    public static void requireByteRange(double value) {
        if (!Double.isFinite(value) || value < Byte.MIN_VALUE || value > Byte.MAX_VALUE) {
            throw new DataException(
                    "Value %s cannot be converted to BYTE (out of range or not finite)".formatted(value));
        }
    }

    public static void requireShortRange(double value) {
        if (!Double.isFinite(value) || value < Short.MIN_VALUE || value > Short.MAX_VALUE) {
            throw new DataException(
                    "Value %s cannot be converted to SHORT (out of range or not finite)".formatted(value));
        }
    }

    public static void requireIntRange(double value) {
        if (!Double.isFinite(value) || value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
            throw new DataException(
                    "Value %s cannot be converted to INT (out of range or not finite)".formatted(value));
        }
    }

    public static void requireLongRange(double value) {
        if (!Double.isFinite(value)) {
            throw new DataException("Value %s cannot be converted to LONG (not finite)".formatted(value));
        }
        // 2^63 is exactly representable as a double; Long.MAX_VALUE = 2^63 - 1 is not, so use the 2^63 boundary
        if (value >= 0x1.0p63 || value < -0x1.0p63) {
            throw new DataException(
                    "Value %s exceeds LONG range [%d, %d]".formatted(value, Long.MIN_VALUE, Long.MAX_VALUE));
        }
    }

    // Catches overflow and non-finite only. We deliberately do not reject finite in-range doubles that lose
    // bit-exact precision when cast to float (e.g. 0.1 → 0.10000000149...), because that would reject very
    // common pipeline values and diverge from Java's standard (float) cast semantics used by other JVM
    // serialization frameworks (Avro, Jackson, Protobuf).
    public static void requireFloatRange(double value) {
        if (!Double.isFinite(value) || Math.abs(value) > Float.MAX_VALUE) {
            throw new DataException("Value %s exceeds FLOAT range or is not finite".formatted(value));
        }
    }
}
