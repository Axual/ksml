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

import io.axual.ksml.data.exception.SchemaException;
import io.axual.ksml.data.mapper.DataTypeDataSchemaMapper;
import io.axual.ksml.data.object.DataBoolean;
import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataLong;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.type.TupleType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TupleSchemaTest {

    @Test
    @DisplayName("TupleSchema constructs a StructSchema with 1 element and proper metadata")
    void constructsWithSingleSubtype() {
        final var mapper = new DataTypeDataSchemaMapper();
        final var type = new TupleType(DataInteger.DATATYPE);

        final var schema = new TupleSchema(type, mapper);

        // Basic naming and metadata
        assertThat(schema.namespace()).isEqualTo(DataSchemaConstants.DATA_SCHEMA_KSML_NAMESPACE);
        assertThat(schema.name()).isEqualTo(type.toString());
        assertThat(schema.fullName()).isEqualTo(DataSchemaConstants.DATA_SCHEMA_KSML_NAMESPACE + "." + type.toString());
        assertThat(schema.hasDoc()).isTrue();
        assertThat(schema.doc()).isEqualTo("Tuple with 1 fields");

        // Fields
        assertThat(schema.fields()).hasSize(1);
        var f0 = schema.field(0);
        assertThat(f0.name()).isEqualTo("elem0");
        assertThat(f0.schema()).isEqualTo(DataSchema.INTEGER_SCHEMA);
        assertThat(schema.field("elem0")).isNotNull();
    }

    @Test
    @DisplayName("TupleSchema constructs fields elem0..elemN-1 with mapped schemas in order")
    void constructsWithMultipleSubtypes() {
        final var mapper = new DataTypeDataSchemaMapper();
        final var type = new TupleType(DataString.DATATYPE, DataLong.DATATYPE, DataBoolean.DATATYPE);

        final var schema = new TupleSchema(type, mapper);

        assertThat(schema.doc()).isEqualTo("Tuple with 3 fields");
        assertThat(schema.fields()).hasSize(3);

        assertThat(schema.field(0).name()).isEqualTo("elem0");
        assertThat(schema.field(0).schema()).isEqualTo(DataSchema.STRING_SCHEMA);

        assertThat(schema.field(1).name()).isEqualTo("elem1");
        assertThat(schema.field(1).schema()).isEqualTo(DataSchema.LONG_SCHEMA);

        assertThat(schema.field(2).name()).isEqualTo("elem2");
        assertThat(schema.field(2).schema()).isEqualTo(DataSchema.BOOLEAN_SCHEMA);
    }

    @Test
    @DisplayName("TupleSchema requires at least one field")
    void throwsOnZeroSubtypes() {
        final var mapper = new DataTypeDataSchemaMapper();
        final var empty = new TupleType();
        assertThatThrownBy(() -> new TupleSchema(empty, mapper))
                .isInstanceOf(SchemaException.class)
                .hasMessageEndingWith("TupleSchema requires at least one field: type=" + empty);
    }
}
