package io.axual.ksml.data.type;

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
import io.axual.ksml.data.schema.*;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class StructTypeTest {

    @Test
    @DisplayName("Default constructor uses name 'Struct', keyType=string, valueType=UNKNOWN; toString equals name")
    void defaultConstructorProperties() {
        StructType t = new StructType();
        assertThat(t)
                .returns(java.util.Map.class, StructType::containerClass)
                .returns("Struct", StructType::toString)
                .returns("Struct", StructType::name);
        assertThat(t.keyType()).isEqualTo(io.axual.ksml.data.object.DataString.DATATYPE);
        assertThat(t.valueType()).isEqualTo(DataType.UNKNOWN);
    }

    @Test
    @DisplayName("Constructor with schema sets name from schema; name() returns schema name; toString equals stored name")
    void constructorWithSchema() {
        StructSchema schema = new StructSchema(DataSchemaConstants.DATA_SCHEMA_KSML_NAMESPACE, "MyStruct", "doc",
                List.of(new DataField("a", DataSchema.STRING_SCHEMA, null, 0)));
        StructType t = new StructType(schema);
        assertThat(t)
                .returns("MyStruct", StructType::toString)
                .returns("MyStruct", StructType::name);
    }

    @Test
    @DisplayName("constructor with explicit name sets that name when schema is null")
    void constructorWithExplicitName() {
        StructType t = new StructType("Order");
        assertThat(t)
                .returns("Order", StructType::toString)
                .returns("Order", StructType::name);
    }

    @Test
    @DisplayName("fieldType returns incaseNoSchema when no schema; maps field type when schema present; incaseNoSuchField when missing")
    void fieldTypeBehavior() {
        // No schema -> returns incaseNoSchema
        StructType noSchema = new StructType();
        assertThat(noSchema.fieldType("x", DataType.UNKNOWN, DataType.UNKNOWN)).isEqualTo(DataType.UNKNOWN);

        // With schema
        DataField f1 = new DataField("s", DataSchema.STRING_SCHEMA, null, 0);
        DataField f2 = new DataField("i", DataSchema.INTEGER_SCHEMA, null, 1);
        StructSchema schema = new StructSchema("ns", "S", null, List.of(f1, f2));
        StructType typed = new StructType(schema);
        // Existing field maps to correct DataType
        assertThat(typed.fieldType("s", DataType.UNKNOWN, DataType.UNKNOWN)).isEqualTo(io.axual.ksml.data.object.DataString.DATATYPE);
        assertThat(typed.fieldType("i", DataType.UNKNOWN, DataType.UNKNOWN)).isEqualTo(io.axual.ksml.data.object.DataInteger.DATATYPE);
        // Missing field returns incaseNoSuchField
        assertThat(typed.fieldType("missing", DataType.UNKNOWN, DataType.UNKNOWN)).isEqualTo(DataType.UNKNOWN);
    }

    @Test
    @DisplayName("isAssignableFrom allows DataNull.DATATYPE; without schema defers to ComplexType; with schema uses schema assignability")
    void assignabilityBehavior() {
        // Without schema both are assignable due to same container and subtypes
        StructType a = new StructType();
        StructType b = new StructType();
        assertThat(a.isAssignableFrom(b)).isTrue();
        assertThat(b.isAssignableFrom(a)).isTrue();

        // Accept DataNull
        assertThat(a.isAssignableFrom(DataNull.DATATYPE)).isTrue();

        // With schema: require other schema to have at least fields without defaults
        DataField req = new DataField("r", DataSchema.STRING_SCHEMA, null, 0); // required, no default
        StructSchema schemaA = new StructSchema("ns", "A", null, List.of(req));
        StructType tA = new StructType(schemaA);
        // Other schema missing the required field -> not assignable
        StructSchema schemaB = new StructSchema("ns", "B", null, List.of());
        StructType tB = new StructType(schemaB);
        assertThat(tA.isAssignableFrom(tB)).isFalse();

        // Other schema with the required field -> assignable
        StructSchema schemaC = new StructSchema("ns", "C", null, List.of(req));
        StructType tC = new StructType(schemaC);
        assertThat(tA.isAssignableFrom(tC)).isTrue();
    }

    @Test
    @DisplayName("equals uses mutual assignability and ComplexType equality; hashCode consistent per instance")
    void equalsAndHashCode() {
        StructType s1 = new StructType();
        StructType s2 = new StructType();
        StructType s3 = new StructType("Other");
        SoftAssertions softly = new SoftAssertions();
        softly.assertThat(s1.equals(s1)).isTrue();
        softly.assertThat(s1).isEqualTo(s2);
        softly.assertThat(s1).isNotEqualTo(s3);
        softly.assertThat(s1.hashCode()).isEqualTo(s1.hashCode());
        softly.assertThat(s3.hashCode()).isEqualTo(s3.hashCode());
        softly.assertAll();
    }
}
