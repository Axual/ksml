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
import io.axual.ksml.data.exception.SchemaException;
import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataLong;
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataString;
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
        assertThat(LogicalTypeRegistry.byName("uuid")).isSameAs(LogicalTypeRegistry.UUID);
        assertThat(LogicalTypeRegistry.byName("time-millis")).isSameAs(LogicalTypeRegistry.TIME_MILLIS);
        assertThat(LogicalTypeRegistry.byName("timestamp-micros")).isSameAs(LogicalTypeRegistry.TIMESTAMP_MICROS);
        assertThat(LogicalTypeRegistry.byName("something-custom")).isNull();
        assertThat(LogicalTypeRegistry.byName(null)).isNull();
    }

    @Test
    @DisplayName("uuid validation accepts a valid UUID, null, and rejects a malformed string")
    void uuid_validation() {
        assertThatCode(() -> LogicalTypeRegistry.UUID.validate(new DataString("123e4567-e89b-12d3-a456-426614174000")))
                .doesNotThrowAnyException();
        assertThatCode(() -> LogicalTypeRegistry.UUID.validate(new DataString(null))).doesNotThrowAnyException();
        assertThatCode(() -> LogicalTypeRegistry.UUID.validate(DataNull.INSTANCE)).doesNotThrowAnyException();
        assertThatThrownBy(() -> LogicalTypeRegistry.UUID.validate(new DataString("not-a-uuid")))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("not a valid uuid");
    }

    @ParameterizedTest
    @DisplayName("time-millis accepts values inside [0, 86_399_999]")
    @ValueSource(ints = {0, 1, 3723000, 86_399_999})
    void timeMillis_accepts_inRange(int value) {
        assertThatCode(() -> LogicalTypeRegistry.TIME_MILLIS.validate(new DataInteger(value))).doesNotThrowAnyException();
    }

    @ParameterizedTest
    @DisplayName("time-millis rejects values outside [0, 86_399_999]")
    @ValueSource(ints = {-1, 86_400_000, Integer.MAX_VALUE, Integer.MIN_VALUE})
    void timeMillis_rejects_outOfRange(int value) {
        assertThatThrownBy(() -> LogicalTypeRegistry.TIME_MILLIS.validate(new DataInteger(value)))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("time-millis");
    }

    @ParameterizedTest
    @DisplayName("time-micros accepts values inside [0, 86_399_999_999]")
    @ValueSource(longs = {0L, 1L, 86_399_999_999L})
    void timeMicros_accepts_inRange(long value) {
        assertThatCode(() -> LogicalTypeRegistry.TIME_MICROS.validate(new DataLong(value))).doesNotThrowAnyException();
    }

    @ParameterizedTest
    @DisplayName("time-micros rejects values outside [0, 86_399_999_999]")
    @ValueSource(longs = {-1L, 86_400_000_000L, Long.MAX_VALUE})
    void timeMicros_rejects_outOfRange(long value) {
        assertThatThrownBy(() -> LogicalTypeRegistry.TIME_MICROS.validate(new DataLong(value)))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("time-micros");
    }

    @Test
    @DisplayName("date and timestamp logical types preserve any value without complaint")
    void preservationOnly_types_acceptAnyValue() {
        assertThatCode(() -> LogicalTypeRegistry.DATE.validate(new DataInteger(-1))).doesNotThrowAnyException();
        assertThatCode(() -> LogicalTypeRegistry.TIMESTAMP_MILLIS.validate(new DataLong(-1L))).doesNotThrowAnyException();
        assertThatCode(() -> LogicalTypeRegistry.LOCAL_TIMESTAMP_MICROS.validate(new DataLong(Long.MAX_VALUE))).doesNotThrowAnyException();
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
        assertThatCode(() -> decimal.validate(new DataString("0.00"))).doesNotThrowAnyException();
        assertThatCode(() -> decimal.validate(new DataString(null))).doesNotThrowAnyException();
        assertThatCode(() -> decimal.validate(DataNull.INSTANCE)).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("decimal(10,2) rejects precision overflow, too many fraction digits, and non-string values")
    void decimal_rejects_invalid() {
        final var decimal = new DecimalLogicalType(10, 2);
        assertThatThrownBy(() -> decimal.validate(new DataString("123456789.99")))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("precision");
        assertThatThrownBy(() -> decimal.validate(new DataString("1.234")))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("fraction digits");
        assertThatThrownBy(() -> decimal.validate(new DataString("not-a-number")))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("not a valid decimal");
        assertThatThrownBy(() -> decimal.validate(new DataInteger(5)))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("must be a string");
    }
}
