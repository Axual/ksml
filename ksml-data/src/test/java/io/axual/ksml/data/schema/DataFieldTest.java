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

import io.axual.ksml.data.exception.DataException;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.axual.ksml.data.schema.DataSchemaConstants.NO_TAG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DataFieldTest {

    @Test
    @DisplayName("Default constructor chain sets required=true, tag=NO_TAG, order=ASC and name anonymous")
    void defaultConstructorChain() {
        assertThat(new DataField(DataSchema.STRING_SCHEMA))
                .returns(null, DataField::name)
                .returns(DataSchema.STRING_SCHEMA, DataField::schema)
                .returns(NO_TAG, DataField::tag)
                .returns(true, DataField::required)
                .returns(false, DataField::constant)
                .returns(null, DataField::defaultValue)
                .returns(DataField.Order.ASCENDING, DataField::order)
                .hasToString("<anonymous>: string (-1)");
    }

    @Test
    @DisplayName("Constructor with required=false marks field optional and keeps tag")
    void optionalFieldConstructor() {
        assertThat(new DataField("age", DataSchema.INTEGER_SCHEMA, "Age of user", 42, false))
                .returns("age", DataField::name)
                .returns(DataSchema.INTEGER_SCHEMA, DataField::schema)
                .returns(42, DataField::tag)
                .returns(false, DataField::required)
                .returns(false, DataField::constant)
                .returns(null, DataField::defaultValue)
                .returns(DataField.Order.ASCENDING, DataField::order)
                .hasToString("age: integer (42, optional)");
    }

    @Test
    @DisplayName("UnionSchema forces field tag to NO_TAG regardless of provided tag")
    void unionSchemaForcesNoTag() {
        final var union = new UnionSchema(new UnionSchema.Member("s", DataSchema.STRING_SCHEMA, 1));
        assertThat(new DataField("u", union, "Union field", 99))
                .returns(NO_TAG, DataField::tag)
                .hasToString("u: union (-1)");
    }

    @Test
    @DisplayName("Required field with explicit DataValue(null) throws DataException")
    void requiredFieldWithNullDefaultThrows() {
        final var nullValue = new DataValue(null);
        assertThatThrownBy(() -> new DataField("name", DataSchema.STRING_SCHEMA, null, 0, true, false, nullValue))
                .isInstanceOf(DataException.class)
                .hasMessageEndingWith("Default value for field \"name\" can not be null");
    }

    @Test
    @DisplayName("Optional field allows explicit DataValue(null) as default")
    void optionalFieldAllowsNullDefaultValue() {
        final var field = new DataField("name", DataSchema.STRING_SCHEMA, null, 0, false, false, new DataValue(null));
        assertThat(field.defaultValue()).isNotNull();
        assertThat(field.defaultValue().value()).isNull();
    }

    @Test
    @DisplayName("hasDoc is true for non-empty, false for null or empty")
    void hasDocBehavior() {
        final var softly = new SoftAssertions();
        softly.assertThat(new DataField("x", DataSchema.STRING_SCHEMA, null, 0).hasDoc()).isFalse();
        softly.assertThat(new DataField("x", DataSchema.STRING_SCHEMA, "", 0).hasDoc()).isFalse();
        softly.assertThat(new DataField("x", DataSchema.STRING_SCHEMA, "doc", 0).hasDoc()).isTrue();
        softly.assertAll();
    }

    @Test
    @DisplayName("isAssignableFrom delegates to underlying schema")
    void isAssignableFromBehavior() {
        final var target = new DataField("i", DataSchema.INTEGER_SCHEMA, null, 0);
        // other with long type is compatible (integer group)
        final var otherInt = new DataField("l", DataSchema.LONG_SCHEMA, null, 0);
        assertThat(target.isAssignableFrom(otherInt).isAssignable()).isTrue();
        // float is not compatible with integer
        final var otherFloat = new DataField("f", DataSchema.FLOAT_SCHEMA, null, 0);
        assertThat(target.isAssignableFrom(otherFloat).isAssignable()).isFalse();
        // null is not assignable
        assertThat(target.isAssignableFrom(null).isAssignable()).isFalse();
    }
}
