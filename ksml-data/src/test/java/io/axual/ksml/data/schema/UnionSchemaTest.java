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

import static org.assertj.core.api.Assertions.assertThat;

class UnionSchemaTest {

    @Test
    @DisplayName("recursively flattens nested unions and supports contains")
    void flatteningAndContains() {
        var intField = new DataField("i", DataSchema.INTEGER_SCHEMA, null, 1);
        var strField = new DataField("s", DataSchema.STRING_SCHEMA, null, 2);
        var boolField = new DataField("b", DataSchema.BOOLEAN_SCHEMA, null, 3);

        var nested = new UnionSchema(strField, boolField);
        var union = new UnionSchema(intField, new DataField(null, nested));

        assertThat(union.memberSchemas()).hasSize(3);
        assertThat(union.contains(DataSchema.INTEGER_SCHEMA)).isTrue();
        assertThat(union.contains(DataSchema.STRING_SCHEMA)).isTrue();
        assertThat(union.contains(DataSchema.BOOLEAN_SCHEMA)).isTrue();
        assertThat(union.contains(DataSchema.FLOAT_SCHEMA)).isFalse();
    }

    @Test
    @DisplayName("isAssignableFrom supports single schema and union with matching name/tag rules")
    void assignabilityRules() {
        var intField = new DataField("i", DataSchema.INTEGER_SCHEMA, null, 1);
        var strField = new DataField("s", DataSchema.STRING_SCHEMA, null, 2);
        var union = new UnionSchema(intField, strField);

        // Single schema assignability
        assertThat(union.isAssignableFrom(DataSchema.INTEGER_SCHEMA)).isTrue();
        assertThat(union.isAssignableFrom(DataSchema.FLOAT_SCHEMA)).isFalse();

        // Other union with matching names/tags
        var other = new UnionSchema(
                new DataField("i", DataSchema.LONG_SCHEMA, null, 1), // compatible by integer group and tag/name match
                new DataField("s", DataSchema.STRING_SCHEMA, null, 2)
        );
        assertThat(union.isAssignableFrom(other)).isTrue();

        // Mismatched tag prevents assignment
        var wrongTag = new UnionSchema(
                new DataField("i", DataSchema.LONG_SCHEMA, null, 99),
                new DataField("s", DataSchema.STRING_SCHEMA, null, 2)
        );
        assertThat(union.isAssignableFrom(wrongTag)).isFalse();

        // Anonymous fields or NO_TAG allow assignment regardless of mismatch
        var anonymous = new UnionSchema(
                new DataField(null, DataSchema.LONG_SCHEMA, null, DataField.NO_TAG),
                new DataField("s", DataSchema.STRING_SCHEMA, null, 2)
        );
        assertThat(union.isAssignableFrom(anonymous)).isTrue();
    }
}
