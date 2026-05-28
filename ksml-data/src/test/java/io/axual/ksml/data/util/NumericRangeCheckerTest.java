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
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NumericRangeCheckerTest {

    // --- requireByteRange(long) ---

    @Test
    void requireByteRange_long_throwsWithHintForOverflow() {
        assertThatThrownBy(() -> NumericRangeChecker.requireByteRange(200L))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("exceeds BYTE range")
                .hasMessageContaining("use 'short', 'int' or 'long' type in schema");
    }

    @Test
    void requireByteRange_long_acceptsBoundaryValues() {
        assertThatCode(() -> NumericRangeChecker.requireByteRange((long) Byte.MIN_VALUE)).doesNotThrowAnyException();
        assertThatCode(() -> NumericRangeChecker.requireByteRange((long) Byte.MAX_VALUE)).doesNotThrowAnyException();
    }

    // --- requireShortRange(long) ---

    @Test
    void requireShortRange_long_throwsWithHintForOverflow() {
        assertThatThrownBy(() -> NumericRangeChecker.requireShortRange(100_000L))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("exceeds SHORT range")
                .hasMessageContaining("use 'int' or 'long' type in schema");
    }

    @Test
    void requireShortRange_long_acceptsBoundaryValues() {
        assertThatCode(() -> NumericRangeChecker.requireShortRange((long) Short.MIN_VALUE)).doesNotThrowAnyException();
        assertThatCode(() -> NumericRangeChecker.requireShortRange((long) Short.MAX_VALUE)).doesNotThrowAnyException();
    }

    // --- requireIntRange(long) ---

    @Test
    void requireIntRange_long_throwsWithHintForOverflow() {
        assertThatThrownBy(() -> NumericRangeChecker.requireIntRange(9_999_999_999L))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("exceeds INT range")
                .hasMessageContaining("use 'long' type in schema");
    }

    @Test
    void requireIntRange_long_acceptsBoundaryValues() {
        assertThatCode(() -> NumericRangeChecker.requireIntRange((long) Integer.MIN_VALUE)).doesNotThrowAnyException();
        assertThatCode(() -> NumericRangeChecker.requireIntRange((long) Integer.MAX_VALUE)).doesNotThrowAnyException();
    }

    // --- requireLongRange(double) ---

    @Test
    void requireLongRange_double_throwsWithHintForOverflow() {
        assertThatThrownBy(() -> NumericRangeChecker.requireLongRange(1.0e20))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("exceeds LONG range")
                .hasMessageContaining("ensure values fit in LONG range");
    }

    @Test
    void requireLongRange_double_throwsForNonFinite() {
        assertThatThrownBy(() -> NumericRangeChecker.requireLongRange(Double.NaN))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("not finite");
        assertThatThrownBy(() -> NumericRangeChecker.requireLongRange(Double.POSITIVE_INFINITY))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("not finite");
    }

    @Test
    void requireLongRange_double_acceptsValuesWithinRange() {
        assertThatCode(() -> NumericRangeChecker.requireLongRange(0.0)).doesNotThrowAnyException();
        assertThatCode(() -> NumericRangeChecker.requireLongRange(1.0e18)).doesNotThrowAnyException();
        assertThatCode(() -> NumericRangeChecker.requireLongRange(-1.0e18)).doesNotThrowAnyException();
    }

    // --- requireFloatRange(double) ---

    @Test
    void requireFloatRange_throwsWithBoundsForFiniteOverflow() {
        assertThatThrownBy(() -> NumericRangeChecker.requireFloatRange(1.0e40))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("exceeds FLOAT range")
                .hasMessageContaining(String.valueOf(-Float.MAX_VALUE))
                .hasMessageContaining(String.valueOf(Float.MAX_VALUE));
    }

    @Test
    void requireFloatRange_passesNaNAndInfinity() {
        assertThatCode(() -> NumericRangeChecker.requireFloatRange(Double.NaN)).doesNotThrowAnyException();
        assertThatCode(() -> NumericRangeChecker.requireFloatRange(Double.POSITIVE_INFINITY)).doesNotThrowAnyException();
        assertThatCode(() -> NumericRangeChecker.requireFloatRange(Double.NEGATIVE_INFINITY)).doesNotThrowAnyException();
    }

    @Test
    void requireFloatRange_acceptsValuesWithinRange() {
        assertThatCode(() -> NumericRangeChecker.requireFloatRange(0.0)).doesNotThrowAnyException();
        assertThatCode(() -> NumericRangeChecker.requireFloatRange(Float.MAX_VALUE)).doesNotThrowAnyException();
        assertThatCode(() -> NumericRangeChecker.requireFloatRange(-Float.MAX_VALUE)).doesNotThrowAnyException();
    }

    // --- convert methods ---

    @Test
    void convertLongToByte_returnsCorrectValue() {
        assert NumericRangeChecker.convertLongToByte(42L) == (byte) 42;
        assert NumericRangeChecker.convertLongToByte((long) Byte.MIN_VALUE) == Byte.MIN_VALUE;
        assert NumericRangeChecker.convertLongToByte((long) Byte.MAX_VALUE) == Byte.MAX_VALUE;
    }

    @Test
    void convertLongToByte_throwsForOutOfRange() {
        assertThatThrownBy(() -> NumericRangeChecker.convertLongToByte(200L)).isInstanceOf(DataException.class);
    }

    @Test
    void convertLongToShort_returnsCorrectValue() {
        assert NumericRangeChecker.convertLongToShort(1000L) == (short) 1000;
        assert NumericRangeChecker.convertLongToShort((long) Short.MIN_VALUE) == Short.MIN_VALUE;
        assert NumericRangeChecker.convertLongToShort((long) Short.MAX_VALUE) == Short.MAX_VALUE;
    }

    @Test
    void convertLongToShort_throwsForOutOfRange() {
        assertThatThrownBy(() -> NumericRangeChecker.convertLongToShort(100_000L)).isInstanceOf(DataException.class);
    }

    @Test
    void convertDoubleToByte_returnsCorrectValue() {
        assert NumericRangeChecker.convertDoubleToByte(42.9) == (byte) 42;
    }

    @Test
    void convertDoubleToByte_throwsForOutOfRange() {
        assertThatThrownBy(() -> NumericRangeChecker.convertDoubleToByte(200.0)).isInstanceOf(DataException.class);
    }

    @Test
    void convertDoubleToShort_returnsCorrectValue() {
        assert NumericRangeChecker.convertDoubleToShort(1000.5) == (short) 1000;
    }

    @Test
    void convertDoubleToShort_throwsForOutOfRange() {
        assertThatThrownBy(() -> NumericRangeChecker.convertDoubleToShort(100_000.0)).isInstanceOf(DataException.class);
    }

    @Test
    void convertDoubleToInt_returnsCorrectValue() {
        assert NumericRangeChecker.convertDoubleToInt(42.9) == 42;
        assert NumericRangeChecker.convertDoubleToInt((double) Integer.MIN_VALUE) == Integer.MIN_VALUE;
        assert NumericRangeChecker.convertDoubleToInt((double) Integer.MAX_VALUE) == Integer.MAX_VALUE;
    }

    @Test
    void convertDoubleToInt_throwsForOutOfRange() {
        assertThatThrownBy(() -> NumericRangeChecker.convertDoubleToInt(9_999_999_999.0)).isInstanceOf(DataException.class);
    }

    @Test
    void convertDoubleToLong_returnsCorrectValue() {
        assert NumericRangeChecker.convertDoubleToLong(1.0e18) == (long) 1.0e18;
        assert NumericRangeChecker.convertDoubleToLong(0.0) == 0L;
    }

    @Test
    void convertDoubleToLong_throwsForOutOfRange() {
        assertThatThrownBy(() -> NumericRangeChecker.convertDoubleToLong(1.0e20)).isInstanceOf(DataException.class);
        assertThatThrownBy(() -> NumericRangeChecker.convertDoubleToLong(Double.NaN)).isInstanceOf(DataException.class);
    }

    @Test
    void convertDoubleToFloat_returnsCorrectValue() {
        assert NumericRangeChecker.convertDoubleToFloat(1.5) == 1.5f;
        assert NumericRangeChecker.convertDoubleToFloat(0.0) == 0.0f;
    }

    @Test
    void convertDoubleToFloat_throwsForOutOfRange() {
        assertThatThrownBy(() -> NumericRangeChecker.convertDoubleToFloat(1.0e40)).isInstanceOf(DataException.class);
    }
}
