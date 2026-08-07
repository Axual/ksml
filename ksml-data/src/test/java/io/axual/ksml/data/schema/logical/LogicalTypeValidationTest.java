package io.axual.ksml.data.schema.logical;

/*-
 * ========================LICENSE_START=================================
 * KSML
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
import io.axual.ksml.data.exception.SchemaException;
import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataLong;
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataString;

import java.math.BigDecimal;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LogicalTypeValidationTest {

    @Test
    @DisplayName("Registry resolves standard names and returns null for unknown or custom names")
    void registry_resolvesKnownNames_andNullForUnknown() {
        assertThat(LogicalTypeConstants.byName("uuid")).isSameAs(LogicalTypeConstants.UUID_TYPE);
        assertThat(LogicalTypeConstants.byName("time-millis")).isSameAs(LogicalTypeConstants.TIME_MILLIS_TYPE);
        assertThat(LogicalTypeConstants.byName("timestamp-micros")).isSameAs(LogicalTypeConstants.TIMESTAMP_MICROS_TYPE);
        assertThat(LogicalTypeConstants.byName("something-custom")).isNull();
        assertThat(LogicalTypeConstants.byName(null)).isNull();
    }

    @Test
    @DisplayName("uuid validation accepts a valid UUID, null, and rejects a malformed string")
    void uuid_validation() {
        assertThatCode(() -> LogicalTypeConstants.UUID_TYPE.validate(new DataString("123e4567-e89b-12d3-a456-426614174000")))
                .doesNotThrowAnyException();
        assertThatCode(() -> LogicalTypeConstants.UUID_TYPE.validate(new DataString(null))).doesNotThrowAnyException();
        assertThatCode(() -> LogicalTypeConstants.UUID_TYPE.validate(DataNull.INSTANCE)).doesNotThrowAnyException();
        final var invalidUuid = new DataString("not-a-uuid");
        assertThatThrownBy(() -> LogicalTypeConstants.UUID_TYPE.validate(invalidUuid))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("not a valid uuid");
    }

    @ParameterizedTest
    @DisplayName("time-millis accepts values inside [0, 86_399_999]")
    @ValueSource(ints = {0, 1, 3723000, 86_399_999})
    void timeMillis_accepts_inRange(int value) {
        assertThatCode(() -> LogicalTypeConstants.TIME_MILLIS_TYPE.validate(new DataInteger(value))).doesNotThrowAnyException();
    }

    @ParameterizedTest
    @DisplayName("time-millis rejects values outside [0, 86_399_999]")
    @ValueSource(ints = {-1, 86_400_000, Integer.MAX_VALUE, Integer.MIN_VALUE})
    void timeMillis_rejects_outOfRange(int value) {
        final var outOfRange = new DataInteger(value);
        assertThatThrownBy(() -> LogicalTypeConstants.TIME_MILLIS_TYPE.validate(outOfRange))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("time-millis");
    }

    @ParameterizedTest
    @DisplayName("time-micros accepts values inside [0, 86_399_999_999]")
    @ValueSource(longs = {0L, 1L, 86_399_999_999L})
    void timeMicros_accepts_inRange(long value) {
        assertThatCode(() -> LogicalTypeConstants.TIME_MICROS_TYPE.validate(new DataLong(value))).doesNotThrowAnyException();
    }

    @ParameterizedTest
    @DisplayName("time-micros rejects values outside [0, 86_399_999_999]")
    @ValueSource(longs = {-1L, 86_400_000_000L, Long.MAX_VALUE})
    void timeMicros_rejects_outOfRange(long value) {
        final var outOfRange = new DataLong(value);
        assertThatThrownBy(() -> LogicalTypeConstants.TIME_MICROS_TYPE.validate(outOfRange))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("time-micros");
    }

    @Test
    @DisplayName("date and timestamp logical types preserve any value without complaint")
    void preservationOnly_types_acceptAnyValue() {
        assertThatCode(() -> LogicalTypeConstants.DATE_TYPE.validate(new DataInteger(-1))).doesNotThrowAnyException();
        assertThatCode(() -> LogicalTypeConstants.TIMESTAMP_MILLIS_TYPE.validate(new DataLong(-1L))).doesNotThrowAnyException();
        assertThatCode(() -> LogicalTypeConstants.LOCAL_TIMESTAMP_MICROS_TYPE.validate(new DataLong(Long.MAX_VALUE))).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("DecimalLogicalType rejects malformed parameters at construction")
    void decimal_rejectsInvalidParameters() {
        assertThatThrownBy(() -> new DecimalLogicalType(0, 0)).isInstanceOf(SchemaException.class);
        assertThatThrownBy(() -> new DecimalLogicalType(-1, 0)).isInstanceOf(SchemaException.class);
        assertThatThrownBy(() -> new DecimalLogicalType(4, -1)).isInstanceOf(SchemaException.class);
        assertThatThrownBy(() -> new DecimalLogicalType(4, 5)).isInstanceOf(SchemaException.class);
    }

    @Test
    @DisplayName("decimal(10,2) accepts a fitting value and null")
    void decimal_accepts_valid() {
        final var decimal = new DecimalLogicalType(10, 2);
        assertThatCode(() -> decimal.validate(new DataString("123.45"))).doesNotThrowAnyException();
        assertThatCode(() -> decimal.validate(new DataString("-123.45"))).doesNotThrowAnyException();
        assertThatCode(() -> decimal.validate(new DataString("0.00"))).doesNotThrowAnyException();
        assertThatCode(() -> decimal.validate(new DataString("99999999.99"))).doesNotThrowAnyException();
        assertThatCode(() -> decimal.validate(new DataString(null))).doesNotThrowAnyException();
        assertThatCode(() -> decimal.validate(DataNull.INSTANCE)).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("toBigDecimal validates and returns the value scaled to the declared scale")
    void decimal_toBigDecimal_scales() {
        final var decimal = new DecimalLogicalType(10, 2);
        assertThat(decimal.toBigDecimal(new DataString("1.5"))).isEqualTo(new BigDecimal("1.50"));
        assertThat(decimal.toBigDecimal(new DataString("-1.2"))).isEqualTo(new BigDecimal("-1.20"));
        assertThat(decimal.toBigDecimal(DataNull.INSTANCE)).isNull();
    }

    @Test
    @DisplayName("decimal(10,2) rejects precision overflow, too many fraction digits, and non-string values")
    void decimal_rejects_invalid() {
        final var decimal = new DecimalLogicalType(10, 2);
        final var precisionOverflow = new DataString("123456789.99");
        assertThatThrownBy(() -> decimal.validate(precisionOverflow))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("precision");
        final var tooManyFractions = new DataString("1.234");
        assertThatThrownBy(() -> decimal.validate(tooManyFractions))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("fraction digits");
        final var notANumber = new DataString("not-a-number");
        assertThatThrownBy(() -> decimal.validate(notANumber))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("not a valid decimal");
        final var nonString = new DataInteger(5);
        assertThatThrownBy(() -> decimal.validate(nonString))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("must be a string");
    }

    @Test
    @DisplayName("A range check works whatever DataObject class carries the number")
    void rangeCheckIgnoresTheWrapperClass() {
        // A Python integer does not always arrive as the exact class of the base type, so checking the
        // number rather than the wrapper is what makes the range check useful in practice.
        final var negativeLong = new DataLong(-1L);
        final var negativeInt = new DataInteger(-1);
        final var validLong = new DataLong(3723000L);

        assertThatThrownBy(() -> LogicalTypeConstants.TIME_MILLIS_TYPE.validate(negativeLong))
                .isInstanceOf(DataException.class);
        assertThatThrownBy(() -> LogicalTypeConstants.TIME_MICROS_TYPE.validate(negativeInt))
                .isInstanceOf(DataException.class);
        assertThatCode(() -> LogicalTypeConstants.TIME_MILLIS_TYPE.validate(validLong))
                .doesNotThrowAnyException();
    }
}
