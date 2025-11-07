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

import io.axual.ksml.data.object.DataNull;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.axual.ksml.data.schema.DataSchemaConstants.NO_TAG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertSame;

class StructFieldTest {

    @Test
    @DisplayName("Default constructor chain sets required=true, tag=NO_TAG, order=ASC and name anonymous")
    void defaultConstructorChain() {
        assertThat(new StructField(DataSchema.STRING_SCHEMA))
                .returns(null, StructField::name)
                .returns(DataSchema.STRING_SCHEMA, StructField::schema)
                .returns(NO_TAG, StructField::tag)
                .returns(true, StructField::required)
                .returns(false, StructField::constant)
                .returns(null, StructField::defaultValue)
                .returns(StructField.Order.ASCENDING, StructField::order)
                .hasToString("<anonymous>: string (-1)");
    }

    @Test
    @DisplayName("Constructor with required=false marks field optional and keeps tag")
    void optionalFieldConstructor() {
        assertThat(new StructField("age", DataSchema.INTEGER_SCHEMA, "Age of user", 42, false))
                .returns("age", StructField::name)
                .returns(DataSchema.INTEGER_SCHEMA, StructField::schema)
                .returns(42, StructField::tag)
                .returns(false, StructField::required)
                .returns(false, StructField::constant)
                .returns(DataNull.INSTANCE, StructField::defaultValue)
                .returns(StructField.Order.ASCENDING, StructField::order)
                .hasToString("age: integer (42, optional)");
    }

    @Test
    @DisplayName("UnionSchema forces field tag to NO_TAG regardless of provided tag")
    void unionSchemaForcesNoTag() {
        final var union = new UnionSchema(new UnionSchema.Member("s", DataSchema.STRING_SCHEMA, null, 1));
        assertThat(new StructField("u", union, "Union field", 99))
                .returns(NO_TAG, StructField::tag)
                .hasToString("u: union (-1)");
    }

    @Test
    @DisplayName("Optional field allows explicit DataValue(null) as default")
    void optionalFieldAllowsNullDefaultValue() {
        final var field = new StructField("name", DataSchema.STRING_SCHEMA, null, 0, false, false, DataNull.INSTANCE);
        assertThat(field.defaultValue()).isNotNull();
        assertSame(DataNull.INSTANCE, field.defaultValue());
    }

    @Test
    @DisplayName("hasDoc is true for non-empty, false for null or empty")
    void hasDocBehavior() {
        final var softly = new SoftAssertions();
        softly.assertThat(new StructField("x", DataSchema.STRING_SCHEMA, null, 0).hasDoc()).isFalse();
        softly.assertThat(new StructField("x", DataSchema.STRING_SCHEMA, "", 0).hasDoc()).isFalse();
        softly.assertThat(new StructField("x", DataSchema.STRING_SCHEMA, "doc", 0).hasDoc()).isTrue();
        softly.assertAll();
    }

    @Test
    @DisplayName("isAssignableFrom delegates to underlying schema")
    void isAssignableFromBehavior() {
        final var target = new StructField("i", DataSchema.INTEGER_SCHEMA, null, 0);
        // other with long type is compatible (integer group)
        final var otherInt = new StructField("l", DataSchema.LONG_SCHEMA, null, 0);
        assertThat(target.isAssignableFrom(otherInt).isAssignable()).isTrue();
        // float is not compatible with integer
        final var otherFloat = new StructField("f", DataSchema.FLOAT_SCHEMA, null, 0);
        assertThat(target.isAssignableFrom(otherFloat).isAssignable()).isFalse();
        // null is not assignable
        assertThat(target.isAssignableFrom(null).isAssignable()).isFalse();
    }
}
