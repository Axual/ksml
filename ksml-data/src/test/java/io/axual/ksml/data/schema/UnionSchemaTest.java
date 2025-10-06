package io.axual.ksml.data.schema;

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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.axual.ksml.data.schema.DataSchemaConstants.NO_TAG;
import static org.assertj.core.api.Assertions.assertThat;

class UnionSchemaTest {

    @Test
    @DisplayName("recursively flattens nested unions and supports contains")
    void flatteningAndContains() {
        final var intField = new UnionSchema.Member("i", DataSchema.INTEGER_SCHEMA, 1);
        final var strField = new UnionSchema.Member("s", DataSchema.STRING_SCHEMA, 2);
        final var boolField = new UnionSchema.Member("b", DataSchema.BOOLEAN_SCHEMA, 3);

        final var nested = new UnionSchema(strField, boolField);
        final var union = new UnionSchema(intField, new UnionSchema.Member(nested));

        assertThat(union.members()).hasSize(3);
        assertThat(union.contains(DataSchema.INTEGER_SCHEMA)).isTrue();
        assertThat(union.contains(DataSchema.STRING_SCHEMA)).isTrue();
        assertThat(union.contains(DataSchema.BOOLEAN_SCHEMA)).isTrue();
        assertThat(union.contains(DataSchema.FLOAT_SCHEMA)).isFalse();
    }

    @Test
    @DisplayName("checkAssignableFrom supports single schema and union with matching name/tag rules")
    void assignabilityRules() {
        final var intField = new UnionSchema.Member("i", DataSchema.INTEGER_SCHEMA, 1);
        final var strField = new UnionSchema.Member("s", DataSchema.STRING_SCHEMA, 2);
        final var union = new UnionSchema(intField, strField);

        // Single schema assignability
        assertThat(union.checkAssignableFrom(DataSchema.INTEGER_SCHEMA).isOK()).isTrue();
        assertThat(union.checkAssignableFrom(DataSchema.FLOAT_SCHEMA).isOK()).isFalse();

        // Other union with matching names/tags
        final var other = new UnionSchema(
                new UnionSchema.Member("i", DataSchema.LONG_SCHEMA, 1), // compatible by integer group and tag/name match
                new UnionSchema.Member("s", DataSchema.STRING_SCHEMA, 2)
        );
        assertThat(union.checkAssignableFrom(other).isOK()).isTrue();

        // Mismatched tag prevents assignment
        final var wrongTag = new UnionSchema(
                new UnionSchema.Member("i", DataSchema.LONG_SCHEMA, 99),
                new UnionSchema.Member("s", DataSchema.STRING_SCHEMA, 2)
        );
        assertThat(union.checkAssignableFrom(wrongTag).isOK()).isFalse();

        // Anonymous fields or NO_TAG allow assignment regardless of mismatch
        final var anonymous = new UnionSchema(
                new UnionSchema.Member(null, DataSchema.LONG_SCHEMA, NO_TAG),
                new UnionSchema.Member("s", DataSchema.STRING_SCHEMA, 2)
        );
        assertThat(union.checkAssignableFrom(anonymous).isOK()).isTrue();
    }
}
