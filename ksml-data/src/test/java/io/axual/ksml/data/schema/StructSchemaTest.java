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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StructSchemaTest {

    private static DataField requiredInt(String name) {
        return new DataField(name, DataSchema.INTEGER_SCHEMA, null, 0);
        // required=true; defaultValue=null
    }

    private static DataField optionalStringWithDefault(String name) {
        return new DataField(name, DataSchema.STRING_SCHEMA, null, 0, true, false, new DataValue("n/a"));
    }

    @Test
    @DisplayName("field accessors and fields() copy")
    void fieldAccessors() {
        var s = new StructSchema("ns", "Person", null, List.of(requiredInt("id"), optionalStringWithDefault("name")));
        assertThat(s.field(0).name()).isEqualTo("id");
        assertThat(s.field("name")).isNotNull();
        var originalSize = s.fields().size();
        // Modify returned list and ensure struct internal state unaffected
        var list = s.fields();
        list.add(requiredInt("age"));
        assertThat(s.fields()).hasSize(originalSize);
        assertThat(s.field("age")).isNull();
    }

    @Test
    @DisplayName("isAssignableFrom validates required presence, defaulted optional omission, and type compatibility")
    void assignabilityRules() {
        var target = new StructSchema("ns", "Person", null, List.of(
                requiredInt("id"),
                optionalStringWithDefault("name")
        ));

        // Other has id as long (compatible) and omits name (allowed due to default)
        var other1 = new StructSchema("ns", "PersonOther", null, List.of(
                new DataField("id", DataSchema.LONG_SCHEMA, null, 0)
        ));
        assertThat(target.isAssignableFrom(other1)).isTrue();

        // Missing required id -> not assignable
        var other2 = new StructSchema("ns", "PersonOther", null, List.of(
                optionalStringWithDefault("name")
        ));
        assertThat(target.isAssignableFrom(other2)).isFalse();

        // Present name with incompatible type -> not assignable
        var other3 = new StructSchema("ns", "PersonOther", null, List.of(
                requiredInt("id"),
                new DataField("name", DataSchema.FLOAT_SCHEMA, null, 0)
        ));
        assertThat(target.isAssignableFrom(other3)).isFalse();

        // Other may contain extra fields -> still assignable
        var other4 = new StructSchema("ns", "PersonOther", null, List.of(
                requiredInt("id"),
                optionalStringWithDefault("name"),
                new DataField("extra", DataSchema.STRING_SCHEMA, null, 0)
        ));
        assertThat(target.isAssignableFrom(other4)).isTrue();
    }

    @Test
    @DisplayName("equals and hashCode consider mutual assignability")
    void equalsAndHashCode() {
        var a = new StructSchema("ns", "A", null, List.of(
                requiredInt("id"),
                optionalStringWithDefault("name")
        ));
        // b omits optional name -> still equal due to mutual assignability
        var b = new StructSchema("ns", "B", null, List.of(
                requiredInt("id")
        ));
        assertThat(a).isNotEqualTo(b);
        assertThat(a.hashCode()).isNotEqualTo(b.hashCode());

        // c has incompatible type for name -> not equal
        var c = new StructSchema("ns", "C", null, List.of(
                requiredInt("id"),
                new DataField("name", DataSchema.FLOAT_SCHEMA, null, 0)
        ));
        assertThat(a).isNotEqualTo(c);
    }

    @Test
    @DisplayName("additionalFieldsAllowed and additionalFieldsSchema invariant")
    void testAdditionalFieldsInvariant() {
        // Test 1: When additionalFieldsAllowed is false, additionalFieldsSchema should be null
        var strictSchema = new StructSchema("ns", "StrictPerson", "doc",
            List.of(requiredInt("id")), false, null);

        assertThat(strictSchema.additionalFieldsAllowed())
            .as("additionalFieldsAllowed should be false")
            .isFalse();
        assertThat(strictSchema.additionalFieldsSchema())
            .as("additionalFieldsSchema should be null when additionalFieldsAllowed is false")
            .isNull();

        // Test 2: When additionalFieldsAllowed is false with null schema
        var strictSchemaWithNull = new StructSchema("ns", "StrictPerson2", "doc",
            List.of(requiredInt("id")), false, null);

        assertThat(strictSchemaWithNull.additionalFieldsAllowed())
            .as("additionalFieldsAllowed should be false")
            .isFalse();
        assertThat(strictSchemaWithNull.additionalFieldsSchema())
            .as("additionalFieldsSchema should be null")
            .isNull();

        // Test 3: When additionalFieldsAllowed is true with specific schema
        var flexibleSchemaWithType = new StructSchema("ns", "FlexiblePerson", "doc",
            List.of(requiredInt("id")), true, DataSchema.STRING_SCHEMA);

        assertThat(flexibleSchemaWithType.additionalFieldsAllowed())
            .as("additionalFieldsAllowed should be true")
            .isTrue();
        assertThat(flexibleSchemaWithType.additionalFieldsSchema())
            .as("additionalFieldsSchema should be STRING_SCHEMA")
            .isEqualTo(DataSchema.STRING_SCHEMA);

        // Test 4: When additionalFieldsAllowed is true with null schema (defaults to ANY_SCHEMA)
        var flexibleSchemaWithNull = new StructSchema("ns", "FlexiblePerson2", "doc",
            List.of(requiredInt("id")), true, null);

        assertThat(flexibleSchemaWithNull.additionalFieldsAllowed())
            .as("additionalFieldsAllowed should be true")
            .isTrue();
        assertThat(flexibleSchemaWithNull.additionalFieldsSchema())
            .as("additionalFieldsSchema should default to ANY_SCHEMA when true with null")
            .isEqualTo(DataSchema.ANY_SCHEMA);

        // Test 5: Constructor with Boolean null defaults to true
        var defaultSchema = new StructSchema("ns", "DefaultPerson", "doc",
            List.of(requiredInt("id")), null, DataSchema.INTEGER_SCHEMA);

        assertThat(defaultSchema.additionalFieldsAllowed())
            .as("additionalFieldsAllowed should default to true when null")
            .isTrue();
        assertThat(defaultSchema.additionalFieldsSchema())
            .as("additionalFieldsSchema should be INTEGER_SCHEMA")
            .isEqualTo(DataSchema.INTEGER_SCHEMA);
    }

    @Test
    @DisplayName("additionalFields behavior with different constructors")
    void testAdditionalFieldsConstructors() {
        // Test the 4-argument constructor (uses default additionalFieldsAllowed = true)
        var defaultConstructorSchema = new StructSchema("ns", "Person", "doc",
            List.of(requiredInt("id")));

        assertThat(defaultConstructorSchema.additionalFieldsAllowed())
            .as("4-arg constructor should default to additionalFieldsAllowed = true")
            .isTrue();
        assertThat(defaultConstructorSchema.additionalFieldsSchema())
            .as("4-arg constructor should default to ANY_SCHEMA")
            .isEqualTo(DataSchema.ANY_SCHEMA);

        // Test the 5-argument constructor
        var fiveArgConstructorSchema = new StructSchema("ns", "Person", "doc",
            List.of(requiredInt("id")), false);

        assertThat(fiveArgConstructorSchema.additionalFieldsAllowed())
            .as("5-arg constructor with false should have additionalFieldsAllowed = false")
            .isFalse();
        assertThat(fiveArgConstructorSchema.additionalFieldsSchema())
            .as("5-arg constructor with false should have null additionalFieldsSchema")
            .isNull();

        // Test copy constructor preserves additionalFields settings
        var originalSchema = new StructSchema("ns", "Original", "doc",
            List.of(requiredInt("id")), true, DataSchema.STRING_SCHEMA);
        var copiedSchema = new StructSchema(originalSchema);

        assertThat(copiedSchema.additionalFieldsAllowed())
            .as("Copy constructor should preserve additionalFieldsAllowed")
            .isEqualTo(originalSchema.additionalFieldsAllowed());
        assertThat(copiedSchema.additionalFieldsSchema())
            .as("Copy constructor should preserve additionalFieldsSchema")
            .isEqualTo(originalSchema.additionalFieldsSchema());
    }

    @Test
    @DisplayName("Copy constructor preserves additionalFields settings exactly")
    void testCopyConstructorPreservesAdditionalFieldsSettings() {
        // Test strict schema copying
        var strictSchema = new StructSchema("ns", "Strict", "doc",
            List.of(requiredInt("id")), false, null);
        var copiedStrict = new StructSchema(strictSchema);

        assertThat(copiedStrict.additionalFieldsAllowed()).isFalse();
        assertThat(copiedStrict.additionalFieldsSchema()).isNull();

        // Test permissive schema copying
        var permissiveSchema = new StructSchema("ns", "Permissive", "doc",
            List.of(requiredInt("id")), true, DataSchema.INTEGER_SCHEMA);
        var copiedPermissive = new StructSchema(permissiveSchema);

        assertThat(copiedPermissive.additionalFieldsAllowed()).isTrue();
        assertThat(copiedPermissive.additionalFieldsSchema()).isEqualTo(DataSchema.INTEGER_SCHEMA);
    }

    @Test
    @DisplayName("Builder method follows new defaults")
    void testBuilderMethodDefaults() {
        var schemaFromBuilder = StructSchema.builder()
            .namespace("ns")
            .name("Test")
            .fields(List.of(requiredInt("id")))
            .build();

        assertThat(schemaFromBuilder.additionalFieldsAllowed())
            .as("Builder should use new default (true)")
            .isTrue();
        assertThat(schemaFromBuilder.additionalFieldsSchema())
            .as("Builder should default to ANY_SCHEMA")
            .isEqualTo(DataSchema.ANY_SCHEMA);
    }

    @Test
    @DisplayName("Constructor parameter validation throws exception for invalid combinations")
    void testConstructorParameterValidation() {
        // Test that providing a schema with additionalFieldsAllowed=false throws exception
        assertThatThrownBy(() ->
            new StructSchema("ns", "Invalid", "doc",
                List.of(requiredInt("id")), false, DataSchema.STRING_SCHEMA))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("additionalFieldsSchema must be null when additionalFieldsAllowed is false");

        // Test that valid combinations work
        assertThatNoException().isThrownBy(() ->
            new StructSchema("ns", "Valid1", "doc",
                List.of(requiredInt("id")), false, null));

        assertThatNoException().isThrownBy(() ->
            new StructSchema("ns", "Valid2", "doc",
                List.of(requiredInt("id")), true, DataSchema.STRING_SCHEMA));

        assertThatNoException().isThrownBy(() ->
            new StructSchema("ns", "Valid3", "doc",
                List.of(requiredInt("id")), true, null));
    }

    @Test
    @DisplayName("Builder validation follows same rules as constructor")
    void testBuilderParameterValidation() {
        // Test that builder also validates parameter consistency
        assertThatThrownBy(() ->
            StructSchema.builder()
                .namespace("ns")
                .name("Invalid")
                .fields(List.of(requiredInt("id")))
                .additionalFieldsAllowed(false)
                .additionalFieldsSchema(DataSchema.STRING_SCHEMA)
                .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("additionalFieldsSchema must be null when additionalFieldsAllowed is false");
    }

    @Test
    @DisplayName("Null Boolean parameter handling")
    void testNullBooleanParameterHandling() {
        // Test that null Boolean additionalFieldsAllowed defaults to true
        var schema = new StructSchema("ns", "Test", "doc",
            List.of(requiredInt("id")), null, DataSchema.INTEGER_SCHEMA);

        assertThat(schema.additionalFieldsAllowed())
            .as("null Boolean should default to true")
            .isTrue();
        assertThat(schema.additionalFieldsSchema())
            .as("Schema should be preserved when allowed")
            .isEqualTo(DataSchema.INTEGER_SCHEMA);

        // Test null Boolean with null schema defaults to ANY_SCHEMA
        var schema2 = new StructSchema("ns", "Test2", "doc",
            List.of(requiredInt("id")), null, null);

        assertThat(schema2.additionalFieldsAllowed()).isTrue();
        assertThat(schema2.additionalFieldsSchema()).isEqualTo(DataSchema.ANY_SCHEMA);
    }
}
