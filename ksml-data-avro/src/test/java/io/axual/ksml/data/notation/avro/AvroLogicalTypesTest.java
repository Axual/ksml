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
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class AvroLogicalTypesTest {
    private static Schema decimalBytes() {
        return LogicalTypes.decimal(10, 2).addToSchema(Schema.create(Schema.Type.BYTES));
    }

    @Test
    @DisplayName("resolveEffective resolves a logical type through a simple optional union [null, T]")
    void resolveEffective_optionalUnion_resolves() {
        final var union = Schema.createUnion(List.of(Schema.create(Schema.Type.NULL), decimalBytes()));
        assertThat(AvroLogicalTypes.resolveEffective(union)).isInstanceOf(DecimalLogicalType.class);
    }

    @Test
    @DisplayName("resolveEffective returns null for a union mixing a logical branch with other types")
    void resolveEffective_mixedUnion_returnsNull() {
        final var union = Schema.createUnion(List.of(
                Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING), decimalBytes()));
        assertThat(AvroLogicalTypes.resolveEffective(union)).isNull();
    }

    @ParameterizedTest
    @DisplayName("decimal codec round-trips signed, zero, and boundary values")
    @ValueSource(strings = {"123.45", "-1.23", "0.00", "99999999.99"})
    void decimalCodec_roundTrips(String value) {
        final var scaled = new BigDecimal(value).setScale(2);
        final var bytes = AvroLogicalTypes.decimalToBytes(scaled).array();
        assertThat(AvroLogicalTypes.decimalToString(bytes, 2)).isEqualTo(scaled.toPlainString());
    }
}
