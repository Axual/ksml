package io.axual.ksml.data.schema;

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

import io.axual.ksml.data.mapper.DataTypeDataSchemaMapper;
import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataLong;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.schema.logical.DecimalLogicalType;
import io.axual.ksml.data.schema.logical.LogicalTypeRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class LogicalSchemaTest {
    private final DataTypeDataSchemaMapper typeMapper = new DataTypeDataSchemaMapper();

    @Test
    @DisplayName("LogicalSchema reports the base primitive's type and exposes its base schema")
    void reportsBaseType() {
        final var decimal = new LogicalSchema(new DecimalLogicalType(10, 2));
        assertThat(decimal.type()).isEqualTo(DataSchemaConstants.BYTES_TYPE);
        assertThat(decimal.baseSchema()).isEqualTo(DataSchema.BYTES_SCHEMA);

        final var uuid = new LogicalSchema(LogicalTypeRegistry.UUID);
        assertThat(uuid.type()).isEqualTo(DataSchemaConstants.STRING_TYPE);
        assertThat(uuid.baseSchema()).isEqualTo(DataSchema.STRING_SCHEMA);
    }

    @Test
    @DisplayName("Equality distinguishes decimal parameters and never equals a bare primitive")
    void equalitySemantics() {
        assertThat(new LogicalSchema(new DecimalLogicalType(10, 2)))
                .isEqualTo(new LogicalSchema(new DecimalLogicalType(10, 2)))
                .isNotEqualTo(new LogicalSchema(new DecimalLogicalType(10, 4)))
                .isNotEqualTo(DataSchema.BYTES_SCHEMA);
        assertThat(new LogicalSchema(LogicalTypeRegistry.UUID))
                .isEqualTo(new LogicalSchema(LogicalTypeRegistry.UUID))
                .isNotEqualTo(new LogicalSchema(LogicalTypeRegistry.DATE));
    }

    @Test
    @DisplayName("A logical schema is assignable only from the identical logical type")
    void assignabilityRequiresSameLogicalType() {
        final var decimal = new LogicalSchema(new DecimalLogicalType(10, 2));
        assertThat(decimal.isAssignableFrom(new LogicalSchema(new DecimalLogicalType(10, 2))).isAssignable()).isTrue();
        assertThat(decimal.isAssignableFrom(new LogicalSchema(new DecimalLogicalType(10, 4))).isAssignable()).isFalse();
        assertThat(decimal.isAssignableFrom(DataSchema.BYTES_SCHEMA).isAssignable()).isFalse();
    }

    @Test
    @DisplayName("equals is symmetric between a logical schema and its base primitive")
    void equalsIsSymmetric() {
        final var decimal = new LogicalSchema(new DecimalLogicalType(10, 2));
        final var forward = decimal.equals(DataSchema.BYTES_SCHEMA);
        final var backward = DataSchema.BYTES_SCHEMA.equals(decimal);
        assertThat(forward).isEqualTo(backward).isFalse();
    }

    @Test
    @DisplayName("The type mapper surfaces a logical schema as its representation type")
    void mapsToRepresentationType() {
        assertThat(typeMapper.fromDataSchema(new LogicalSchema(new DecimalLogicalType(10, 2))))
                .isEqualTo(DataString.DATATYPE);
        assertThat(typeMapper.fromDataSchema(new LogicalSchema(LogicalTypeRegistry.UUID)))
                .isEqualTo(DataString.DATATYPE);
        assertThat(typeMapper.fromDataSchema(new LogicalSchema(LogicalTypeRegistry.DATE)))
                .isEqualTo(DataInteger.DATATYPE);
        assertThat(typeMapper.fromDataSchema(new LogicalSchema(LogicalTypeRegistry.TIME_MICROS)))
                .isEqualTo(DataLong.DATATYPE);
    }
}
