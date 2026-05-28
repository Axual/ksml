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
 * Range-check helpers for narrowing numeric conversions used by {@code NativeDataObjectMapper}
 * and {@code ConvertUtil}. Each type has a {@code long} overload for integral sources and a
 * {@code double} overload for floating-point sources (which also checks finiteness).
 *
 * <p><b>Internal API.</b> Public only because the two call sites live in different packages.</p>
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class NumericRangeChecker {
    /** Upper bound for raw byte-array paths: 255 (0xFF) legitimately maps to (byte) -1. */
    public static final int UNSIGNED_BYTE_MAX_VALUE = 0xFF;

    // Long.MAX_VALUE = 2^63 - 1 is not exactly representable as a double; the nearest
    // representable double is 2^63 itself, which already overflows a long. Using 2^63
    // as the exclusive upper bound is therefore the correct boundary for range checks.
    private static final double LONG_OVERFLOW_THRESHOLD = 0x1.0p63;

    public static void requireByteRange(long value) {
        if (value < Byte.MIN_VALUE || value > Byte.MAX_VALUE) {
            throw new DataException(
                    "Value %d exceeds BYTE range [%d, %d]; use 'short', 'int' or 'long' type in schema or ensure values fit in BYTE range"
                            .formatted(value, Byte.MIN_VALUE, Byte.MAX_VALUE));
        }
    }

    public static void requireShortRange(long value) {
        if (value < Short.MIN_VALUE || value > Short.MAX_VALUE) {
            throw new DataException(
                    "Value %d exceeds SHORT range [%d, %d]; use 'int' or 'long' type in schema or ensure values fit in SHORT range"
                            .formatted(value, Short.MIN_VALUE, Short.MAX_VALUE));
        }
    }

    public static void requireIntRange(long value) {
        if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
            throw new DataException(
                    "Value %d exceeds INT range [%d, %d]; use 'long' type in schema or ensure values fit in INT range"
                            .formatted(value, Integer.MIN_VALUE, Integer.MAX_VALUE));
        }
    }

    public static void requireByteRange(double value) {
        if (!Double.isFinite(value) || value < Byte.MIN_VALUE || value > Byte.MAX_VALUE) {
            throw new DataException(
                    "Value %s cannot be converted to BYTE (out of range or not finite); use 'short', 'int' or 'long' type in schema or ensure values fit in BYTE range"
                            .formatted(value));
        }
    }

    public static void requireShortRange(double value) {
        if (!Double.isFinite(value) || value < Short.MIN_VALUE || value > Short.MAX_VALUE) {
            throw new DataException(
                    "Value %s cannot be converted to SHORT (out of range or not finite); use 'int' or 'long' type in schema or ensure values fit in SHORT range"
                            .formatted(value));
        }
    }

    public static void requireIntRange(double value) {
        if (!Double.isFinite(value) || value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
            throw new DataException(
                    "Value %s cannot be converted to INT (out of range or not finite); use 'long' type in schema or ensure values fit in INT range"
                            .formatted(value));
        }
    }

    public static void requireLongRange(double value) {
        if (!Double.isFinite(value)) {
            throw new DataException("Value %s cannot be converted to LONG (not finite)".formatted(value));
        }
        if (value >= LONG_OVERFLOW_THRESHOLD || value < -LONG_OVERFLOW_THRESHOLD) {
            throw new DataException(
                    "Value %s exceeds LONG range [%d, %d]; ensure values fit in LONG range or use 'double' type in schema"
                            .formatted(value, Long.MIN_VALUE, Long.MAX_VALUE));
        }
    }

    // Rejects finite values whose magnitude exceeds Float.MAX_VALUE — those would silently clamp to
    // ±Infinity under a plain Java cast. NaN and ±Infinity pass through because the cast preserves them.
    // Precision loss for in-range doubles is accepted to match Java's (float) cast semantics.
    public static void requireFloatRange(double value) {
        if (Double.isFinite(value) && Math.abs(value) > Float.MAX_VALUE) {
            throw new DataException("Value %s exceeds FLOAT range [%s, %s]".formatted(value, -Float.MAX_VALUE, Float.MAX_VALUE));
        }
    }

    public static int convertLongToInt(long value) {
        requireIntRange(value);
        return (int) value;
    }
}
