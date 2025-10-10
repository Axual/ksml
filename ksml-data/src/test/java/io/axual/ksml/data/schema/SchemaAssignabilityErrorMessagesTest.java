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

import io.axual.ksml.data.compare.Assignable;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that validate the detailed error messages produced by schema assignability checks.
 */
@Slf4j
class SchemaAssignabilityErrorMessagesTest {

    @Test
    @DisplayName("Struct schema field type mismatch provides detailed error chain")
    void structFieldTypeMismatchDetailedError() {
        // Create source schema: Person { name: string, age: int }
        var personSchema = StructSchema.builder()
            .name("Person")
            .field(new DataField("name", DataSchema.STRING_SCHEMA))
            .field(new DataField("age", DataSchema.INTEGER_SCHEMA))
            .additionalFieldsAllowed(false)
            .build();

        // Create target schema: User { name: string, age: string } - age has wrong type!
        var userSchema = StructSchema.builder()
            .name("User")
            .field(new DataField("name", DataSchema.STRING_SCHEMA))
            .field(new DataField("age", DataSchema.STRING_SCHEMA))  // Wrong type!
            .additionalFieldsAllowed(false)
            .build();

        // Check assignability
        Assignable result = personSchema.isAssignableFrom(userSchema);

        // Validate failure
        assertThat(result.isNotAssignable())
            .as("Person schema should not be assignable from User schema due to age field type mismatch")
            .isTrue();

        // Validate error message chain
        String errorMessage = result.toString(false);
        assertThat(errorMessage)
            .as("Error message should explain field mismatch")
            .contains("age")
            .containsAnyOf("integer", "Integer")
            .containsAnyOf("string", "String")
            .contains("not assignable");
    }

    @Test
    @DisplayName("Struct schema missing required field provides clear error")
    void structMissingRequiredFieldError() {
        // Create source schema: Person { name: string, age: int, email: string }
        var personSchema = StructSchema.builder()
            .name("Person")
            .field(new DataField("name", DataSchema.STRING_SCHEMA))
            .field(new DataField("age", DataSchema.INTEGER_SCHEMA))
            .field(new DataField("email", DataSchema.STRING_SCHEMA))  // Required field
            .additionalFieldsAllowed(false)
            .build();

        // Create target schema: User { name: string, age: int } - missing email!
        var userSchema = StructSchema.builder()
            .name("User")
            .field(new DataField("name", DataSchema.STRING_SCHEMA))
            .field(new DataField("age", DataSchema.INTEGER_SCHEMA))
            // email field is missing!
            .additionalFieldsAllowed(false)
            .build();

        // Check assignability
        Assignable result = personSchema.isAssignableFrom(userSchema);

        // Validate failure
        assertThat(result.isNotAssignable())
            .as("Person schema should not be assignable from User schema due to missing email field")
            .isTrue();

        // Validate error message
        String errorMessage = result.toString(false);
        assertThat(errorMessage)
            .as("Error message should explain missing required field")
            .contains("email")
            .containsAnyOf("required", "does not contain");
    }

    @Test
    @DisplayName("Union schema member mismatch provides helpful error")
    void unionMemberMismatchDetailedError() {
        var union1 = new UnionSchema(
            new UnionSchema.Member(DataSchema.STRING_SCHEMA),
            new UnionSchema.Member(DataSchema.INTEGER_SCHEMA)
        );

        var union2 = new UnionSchema(
            new UnionSchema.Member(DataSchema.BOOLEAN_SCHEMA),  // Different member!
            new UnionSchema.Member(DataSchema.INTEGER_SCHEMA)
        );

        Assignable result = union1.isAssignableFrom(union2);

        assertThat(result.isNotAssignable())
            .as("Union should not be assignable when members differ")
            .isTrue();

        String errorMessage = result.toString(false);
        assertThat(errorMessage)
            .as("Error message should explain union member mismatch")
            .containsAnyOf("Schema", "mismatch");
    }

    @Test
    @DisplayName("Nested struct mismatch provides multi-level error chain")
    void nestedStructMismatchMultiLevelError() {
        // Create Address { street: string, city: string }
        var addressSchema = StructSchema.builder()
            .name("Address")
            .field(new DataField("street", DataSchema.STRING_SCHEMA))
            .field(new DataField("city", DataSchema.STRING_SCHEMA))
            .additionalFieldsAllowed(false)
            .build();

        // Create Person { name: string, address: Address }
        var personSchema = StructSchema.builder()
            .name("Person")
            .field(new DataField("name", DataSchema.STRING_SCHEMA))
            .field(new DataField("address", addressSchema))
            .additionalFieldsAllowed(false)
            .build();

        // Create wrong Address { street: string, city: int } - city has wrong type!
        var wrongAddressSchema = StructSchema.builder()
            .name("Address")
            .field(new DataField("street", DataSchema.STRING_SCHEMA))
            .field(new DataField("city", DataSchema.INTEGER_SCHEMA))  // Wrong!
            .additionalFieldsAllowed(false)
            .build();

        // Create User { name: string, address: WrongAddress }
        var userSchema = StructSchema.builder()
            .name("User")
            .field(new DataField("name", DataSchema.STRING_SCHEMA))
            .field(new DataField("address", wrongAddressSchema))
            .additionalFieldsAllowed(false)
            .build();

        Assignable result = personSchema.isAssignableFrom(userSchema);

        assertThat(result.isNotAssignable())
            .as("Person schema should not be assignable from User due to nested Address.city type mismatch")
            .isTrue();

        String errorMessage = result.toString(false);
        assertThat(errorMessage)
            .as("Error message should show both top-level and nested field errors")
            .contains("address")  // Top-level field
            .contains("city")     // Nested field
            .containsAnyOf("string", "String")
            .containsAnyOf("integer", "Integer");
    }

    @Test
    @DisplayName("Enum schema assignability with different symbols")
    void enumSchemaDifferentSymbols() {
        var colorEnum = new EnumSchema(
            "io.axual.test",
            "Color",
            "Color enumeration",
            List.of(
                new EnumSchema.Symbol("RED"),
                new EnumSchema.Symbol("GREEN"),
                new EnumSchema.Symbol("BLUE")
            )
        );

        var sizeEnum = new EnumSchema(
            "io.axual.test",
            "Size",
            "Size enumeration",
            List.of(
                new EnumSchema.Symbol("SMALL"),
                new EnumSchema.Symbol("MEDIUM"),
                new EnumSchema.Symbol("LARGE")
            )
        );

        Assignable result = colorEnum.isAssignableFrom(sizeEnum);

        assertThat(result.isNotAssignable())
            .as("Different enum schemas should not be assignable")
            .isTrue();
    }

    @Test
    @DisplayName("List schema element type mismatch")
    void listElementTypeMismatch() {
        var listOfStrings = new ListSchema(DataSchema.STRING_SCHEMA);
        var listOfIntegers = new ListSchema(DataSchema.INTEGER_SCHEMA);

        Assignable result = listOfStrings.isAssignableFrom(listOfIntegers);

        assertThat(result.isNotAssignable())
            .as("List of strings should not be assignable from list of integers")
            .isTrue();

        String errorMessage = result.toString(false);
        assertThat(errorMessage)
            .containsAnyOf("string", "String", "integer", "Integer");
    }

    @Test
    @DisplayName("Map schema value type mismatch")
    void mapValueTypeMismatch() {
        var mapStringToInt = new MapSchema(DataSchema.INTEGER_SCHEMA);
        var mapStringToString = new MapSchema(DataSchema.STRING_SCHEMA);

        Assignable result = mapStringToInt.isAssignableFrom(mapStringToString);

        assertThat(result.isNotAssignable())
            .as("Map with integer values should not be assignable from map with string values")
            .isTrue();

        String errorMessage = result.toString(false);
        assertThat(errorMessage)
            .containsAnyOf("integer", "Integer", "string", "String");
    }

    @Test
    @DisplayName("Complex nested structure with multiple errors")
    void complexNestedStructureMultipleErrors() {
        // Create a complex structure with nested types
        var addressSchema = StructSchema.builder()
            .name("Address")
            .field(new DataField("street", DataSchema.STRING_SCHEMA))
            .field(new DataField("zipCode", DataSchema.INTEGER_SCHEMA))
            .additionalFieldsAllowed(false)
            .build();

        var phoneSchema = StructSchema.builder()
            .name("Phone")
            .field(new DataField("countryCode", DataSchema.STRING_SCHEMA))
            .field(new DataField("number", DataSchema.STRING_SCHEMA))
            .additionalFieldsAllowed(false)
            .build();

        var personSchema = StructSchema.builder()
            .name("Person")
            .field(new DataField("name", DataSchema.STRING_SCHEMA))
            .field(new DataField("age", DataSchema.INTEGER_SCHEMA))
            .field(new DataField("address", addressSchema))
            .field(new DataField("phone", phoneSchema))
            .additionalFieldsAllowed(false)
            .build();

        // Create wrong schemas
        var wrongAddressSchema = StructSchema.builder()
            .name("Address")
            .field(new DataField("street", DataSchema.STRING_SCHEMA))
            .field(new DataField("zipCode", DataSchema.STRING_SCHEMA))  // Wrong type!
            .additionalFieldsAllowed(false)
            .build();

        var wrongPhoneSchema = StructSchema.builder()
            .name("Phone")
            .field(new DataField("countryCode", DataSchema.INTEGER_SCHEMA))  // Wrong type!
            .field(new DataField("number", DataSchema.STRING_SCHEMA))
            .additionalFieldsAllowed(false)
            .build();

        var wrongPersonSchema = StructSchema.builder()
            .name("Person")
            .field(new DataField("name", DataSchema.STRING_SCHEMA))
            .field(new DataField("age", DataSchema.STRING_SCHEMA))  // Wrong type!
            .field(new DataField("address", wrongAddressSchema))
            .field(new DataField("phone", wrongPhoneSchema))
            .additionalFieldsAllowed(false)
            .build();

        Assignable result = personSchema.isAssignableFrom(wrongPersonSchema);

        assertThat(result.isNotAssignable())
            .as("Complex schema with multiple nested errors should not be assignable")
            .isTrue();

        String errorMessage = result.toString(false);
        // The error will report the FIRST mismatch encountered
        assertThat(errorMessage).isNotEmpty();
    }

    @Test
    @DisplayName("Integer type family assignability")
    void integerTypeFamilyAssignability() {
        // Integer types should be assignable among themselves
        assertThat(DataSchema.INTEGER_SCHEMA.isAssignableFrom(DataSchema.BYTE_SCHEMA).isOK())
            .as("INTEGER should be assignable from BYTE")
            .isTrue();

        assertThat(DataSchema.LONG_SCHEMA.isAssignableFrom(DataSchema.SHORT_SCHEMA).isOK())
            .as("LONG should be assignable from SHORT")
            .isTrue();

        // But not from different type families
        var result = DataSchema.INTEGER_SCHEMA.isAssignableFrom(DataSchema.STRING_SCHEMA);
        assertThat(result.isNotAssignable())
            .as("INTEGER should not be assignable from STRING")
            .isTrue();

        String errorMessage = result.toString(false);
        assertThat(errorMessage)
            .contains("mismatch")
            .containsAnyOf("integer", "Integer", "string", "String");
    }

    @Test
    @DisplayName("String schema accepts enum schema")
    void stringAcceptsEnum() {
        var colorEnum = new EnumSchema(
            List.of(
                new EnumSchema.Symbol("RED"),
                new EnumSchema.Symbol("GREEN"),
                new EnumSchema.Symbol("BLUE")
            )
        );

        // String should accept enum (special case)
        Assignable result = DataSchema.STRING_SCHEMA.isAssignableFrom(colorEnum);

        assertThat(result.isOK())
            .as("STRING schema should accept ENUM schema")
            .isTrue();
    }
}
