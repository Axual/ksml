package io.axual.ksml.data.mapper;

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
import io.axual.ksml.data.object.DataBoolean;
import io.axual.ksml.data.object.DataByte;
import io.axual.ksml.data.object.DataBytes;
import io.axual.ksml.data.object.DataDouble;
import io.axual.ksml.data.object.DataFloat;
import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataLong;
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataShort;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.schema.DataField;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.DataSchemaConstants;
import io.axual.ksml.data.schema.EnumSchema;
import io.axual.ksml.data.schema.ListSchema;
import io.axual.ksml.data.schema.MapSchema;
import io.axual.ksml.data.schema.NamedSchema;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.schema.TupleSchema;
import io.axual.ksml.data.schema.UnionSchema;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.EnumType;
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.data.type.MapType;
import io.axual.ksml.data.type.StructType;
import io.axual.ksml.data.type.TupleType;
import io.axual.ksml.data.type.UnionType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.axual.ksml.data.schema.DataSchemaConstants.NO_TAG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DataTypeDataSchemaMapperTest {
    private final DataTypeDataSchemaMapper mapper = new DataTypeDataSchemaMapper();

    @Test
    @DisplayName("toDataSchema maps primitives and special types including BYTE and UNKNOWN correctly")
    void primitiveAndSpecialTypeToSchemaMappings() {
        assertThat(mapper.toDataSchema(DataType.UNKNOWN)).isSameAs(DataSchema.ANY_SCHEMA);
        assertThat(mapper.toDataSchema(DataNull.DATATYPE)).isSameAs(DataSchema.NULL_SCHEMA);
        assertThat(mapper.toDataSchema(DataBoolean.DATATYPE)).isSameAs(DataSchema.BOOLEAN_SCHEMA);
        assertThat(mapper.toDataSchema(DataByte.DATATYPE)).isSameAs(DataSchema.BYTE_SCHEMA);
        assertThat(mapper.toDataSchema(DataShort.DATATYPE)).isSameAs(DataSchema.SHORT_SCHEMA);
        assertThat(mapper.toDataSchema(DataInteger.DATATYPE)).isSameAs(DataSchema.INTEGER_SCHEMA);
        assertThat(mapper.toDataSchema(DataLong.DATATYPE)).isSameAs(DataSchema.LONG_SCHEMA);
        assertThat(mapper.toDataSchema(DataFloat.DATATYPE)).isSameAs(DataSchema.FLOAT_SCHEMA);
        assertThat(mapper.toDataSchema(DataDouble.DATATYPE)).isSameAs(DataSchema.DOUBLE_SCHEMA);
        assertThat(mapper.toDataSchema(DataBytes.DATATYPE)).isSameAs(DataSchema.BYTES_SCHEMA);
        assertThat(mapper.toDataSchema(DataString.DATATYPE)).isSameAs(DataSchema.STRING_SCHEMA);
    }

    @Test
    @DisplayName("fromDataSchema maps supported schemas back to types; ANY/null -> UNKNOWN")
    void schemaToTypeMappingsForSupportedSchemas() {
        assertThat(mapper.fromDataSchema(null)).isSameAs(DataType.UNKNOWN);
        assertThat(mapper.fromDataSchema(DataSchema.ANY_SCHEMA)).isSameAs(DataType.UNKNOWN);
        assertThat(mapper.fromDataSchema(DataSchema.NULL_SCHEMA)).isSameAs(DataNull.DATATYPE);
        assertThat(mapper.fromDataSchema(DataSchema.BOOLEAN_SCHEMA)).isSameAs(DataBoolean.DATATYPE);
        assertThat(mapper.fromDataSchema(DataSchema.SHORT_SCHEMA)).isSameAs(DataShort.DATATYPE);
        assertThat(mapper.fromDataSchema(DataSchema.INTEGER_SCHEMA)).isSameAs(DataInteger.DATATYPE);
        assertThat(mapper.fromDataSchema(DataSchema.LONG_SCHEMA)).isSameAs(DataLong.DATATYPE);
        assertThat(mapper.fromDataSchema(DataSchema.FLOAT_SCHEMA)).isSameAs(DataFloat.DATATYPE);
        assertThat(mapper.fromDataSchema(DataSchema.DOUBLE_SCHEMA)).isSameAs(DataDouble.DATATYPE);
        assertThat(mapper.fromDataSchema(DataSchema.BYTES_SCHEMA)).isSameAs(DataBytes.DATATYPE);
        assertThat(mapper.fromDataSchema(DataSchema.STRING_SCHEMA)).isSameAs(DataString.DATATYPE);
    }

    @Test
    @DisplayName("fromDataSchema for BYTE schema is currently unsupported and throws SchemaException")
    void fromDataSchemaByteUnsupported() {
        assertThatThrownBy(() -> mapper.fromDataSchema(DataSchema.BYTE_SCHEMA))
                .isInstanceOf(SchemaException.class)
                .hasMessageContaining("Can not convert schema");
    }

    @Test
    @DisplayName("EnumType <-> EnumSchema round-trip preserves symbols")
    void enumRoundTripPreservesSymbols() {
        final var allowedSymbols = List.of(new EnumSchema.Symbol("A"), new EnumSchema.Symbol("B"));
        final var enumType = new EnumType(new EnumSchema(allowedSymbols));

        final var enumSchema = mapper.toDataSchema(enumType);
        assertThat(enumSchema).isInstanceOf(EnumSchema.class);
        final var concreteEnumSchema = (EnumSchema) enumSchema;
        assertThat(concreteEnumSchema.symbols()).containsExactlyElementsOf(allowedSymbols);

        final var mappedBackType = mapper.fromDataSchema(enumSchema);
        assertThat(mappedBackType).isInstanceOf(EnumType.class);
        assertThat(((EnumType) mappedBackType).schema().symbols()).containsExactlyElementsOf(allowedSymbols);
    }

    @Test
    @DisplayName("EnumSchema ignores provided namespace and uses default enum name")
    void enumSchemaIgnoresProvidedNamespace() {
        final var allowedSymbols = List.of(new EnumSchema.Symbol("X"), new EnumSchema.Symbol("Y"));
        final var enumType = new EnumType(new EnumSchema(allowedSymbols));

        final var enumSchema = mapper.toDataSchema("custom.ns", "CustomName", enumType);
        assertThat(enumSchema).isInstanceOf(EnumSchema.class);
        final var concreteEnumSchema = (EnumSchema) enumSchema;
        // Namespace is ignored: fullName equals simple name (no namespace prefix)
        assertThat(concreteEnumSchema.fullName()).isEqualTo(concreteEnumSchema.name());
        // The name used is the enum type name ("enum")
        assertThat(concreteEnumSchema.name()).isEqualTo(enumType.toString());
        assertThat(concreteEnumSchema.hasDoc()).isFalse();
    }

    @Test
    @DisplayName("ListType and MapType nesting round-trip produces equivalent types")
    void listAndMapNestedRoundTrip() {
        final var nestedValueType = new MapType(DataString.DATATYPE);
        final var listOfMapsType = new ListType(nestedValueType);

        final var listSchema = mapper.toDataSchema(listOfMapsType);
        assertThat(listSchema).isInstanceOf(ListSchema.class);
        assertThat(((ListSchema) listSchema).valueSchema()).isInstanceOf(MapSchema.class);

        final var roundTrippedType = mapper.fromDataSchema(listSchema);
        assertThat(roundTrippedType).isInstanceOf(ListType.class);
        final var roundListValueType = ((ListType) roundTrippedType).valueType();
        assertThat(roundListValueType).isInstanceOf(MapType.class);
        assertThat(((MapType) roundListValueType).valueType()).isSameAs(DataString.DATATYPE);
    }

    @Test
    @DisplayName("StructType: schemaless maps to SCHEMALESS; with schema round-trips and remains compatible")
    void structTypeSchemalessAndWithSchema() {
        final var schemalessStructType = new StructType();
        final var schemalessSchema = mapper.toDataSchema(schemalessStructType);
        assertThat(schemalessSchema).isSameAs(StructSchema.SCHEMALESS);

        final var nameField = new DataField("name", DataSchema.STRING_SCHEMA);
        final var ageOptionalField = new DataField("age", DataSchema.INTEGER_SCHEMA, null, NO_TAG, false);
        final var personStructSchema = new StructSchema("example", "Person", "A person", List.of(nameField, ageOptionalField));
        final var structTypeWithSchema = new StructType(personStructSchema);

        final var mappedSchema = mapper.toDataSchema(structTypeWithSchema);
        assertThat(mappedSchema).isInstanceOf(StructSchema.class);
        assertThat(((StructSchema) mappedSchema).fields()).containsExactly(nameField, ageOptionalField);

        final var mappedBackType = mapper.fromDataSchema(mappedSchema);
        assertThat(mappedBackType).isInstanceOf(StructType.class);
        // A StructType mapped from SCHEMALESS becomes a StructType with null schema internally
        final var mappedBackFromSchemaless = mapper.fromDataSchema(StructSchema.SCHEMALESS);
        // Mapping back a SCHEMALESS schema yields a StructType that maps to SCHEMALESS again
        assertThat(mapper.toDataSchema((StructType) mappedBackFromSchemaless)).isSameAs(StructSchema.SCHEMALESS);
    }

    @Test
    @DisplayName("TupleType round-trip works and empty TupleType -> TupleSchema throws")
    void tupleTypeRoundTripAndEmptyTupleThrows() {
        final var twoElementTupleType = new TupleType(DataString.DATATYPE, DataInteger.DATATYPE);
        final var tupleSchema = mapper.toDataSchema(twoElementTupleType);
        assertThat(tupleSchema).isInstanceOf(TupleSchema.class);
        final var concreteTupleSchema = (TupleSchema) tupleSchema;
        assertThat(concreteTupleSchema.fields()).hasSize(2);
        assertThat(concreteTupleSchema.field(0).schema()).isSameAs(DataSchema.STRING_SCHEMA);
        assertThat(concreteTupleSchema.field(1).schema()).isSameAs(DataSchema.INTEGER_SCHEMA);

        final var mappedBackType = mapper.fromDataSchema(tupleSchema);
        assertThat(mappedBackType).isInstanceOf(TupleType.class);
        final var mappedBackTuple = (TupleType) mappedBackType;
        assertThat(mappedBackTuple.subTypeCount()).isEqualTo(2);
        assertThat(mappedBackTuple.subType(0)).isSameAs(DataString.DATATYPE);
        assertThat(mappedBackTuple.subType(1)).isSameAs(DataInteger.DATATYPE);

        final var emptyTupleType = new TupleType();
        assertThatThrownBy(() -> mapper.toDataSchema(emptyTupleType))
                .isInstanceOf(SchemaException.class)
                .hasMessageContaining("TupleSchema requires at least one field");
    }

    @Test
    @DisplayName("TupleSchema uses KSML namespace and name equals TupleType.toString() with correct doc")
    void tupleSchemaUsesKsmlNamespaceAndGeneratedName() {
        final var singleElementTupleType = new TupleType(DataString.DATATYPE);
        final var tupleSchema = mapper.toDataSchema(singleElementTupleType);
        assertThat(tupleSchema).isInstanceOf(TupleSchema.class);
        final var named = (NamedSchema) tupleSchema;
        assertThat(named.fullName()).startsWith(DataSchemaConstants.DATA_SCHEMA_KSML_NAMESPACE + ".");
        assertThat(named.name()).isEqualTo(singleElementTupleType.toString());
        assertThat(named.hasDoc()).isTrue();
        assertThat(named.toString()).contains(singleElementTupleType.toString());
    }

    @Test
    @DisplayName("UnionType round-trip preserves member order, names and tags")
    void unionTypeRoundTripPreservesMemberMetadata() {
        final var memberInt = new UnionType.Member("intField", DataInteger.DATATYPE, 1);
        final var memberString = new UnionType.Member("stringField", DataString.DATATYPE, 2);
        final var unionType = new UnionType(memberInt, memberString);

        final var unionSchema = mapper.toDataSchema(unionType);
        assertThat(unionSchema).isInstanceOf(UnionSchema.class);
        final var concreteUnionSchema = (UnionSchema) unionSchema;
        assertThat(concreteUnionSchema.members()).hasSize(2);
        assertThat(concreteUnionSchema.members()[0].name()).isEqualTo("intField");
        assertThat(concreteUnionSchema.members()[0].tag()).isEqualTo(1);
        assertThat(concreteUnionSchema.members()[0].schema()).isSameAs(DataSchema.INTEGER_SCHEMA);
        assertThat(concreteUnionSchema.members()[1].name()).isEqualTo("stringField");
        assertThat(concreteUnionSchema.members()[1].tag()).isEqualTo(2);
        assertThat(concreteUnionSchema.members()[1].schema()).isSameAs(DataSchema.STRING_SCHEMA);

        final var mappedBackUnion = mapper.fromDataSchema(unionSchema);
        assertThat(mappedBackUnion).isInstanceOf(UnionType.class);
        final var mappedMembers = ((UnionType) mappedBackUnion).members();
        assertThat(mappedMembers).hasSize(2);
        assertThat(mappedMembers[0].name()).isEqualTo("intField");
        assertThat(mappedMembers[0].tag()).isEqualTo(1);
        assertThat(mappedMembers[0].type()).isSameAs(DataInteger.DATATYPE);
        assertThat(mappedMembers[1].name()).isEqualTo("stringField");
        assertThat(mappedMembers[1].tag()).isEqualTo(2);
        assertThat(mappedMembers[1].type()).isSameAs(DataString.DATATYPE);
    }

    @Test
    @DisplayName("Empty UnionType round-trip yields zero members")
    void emptyUnionTypeRoundTrip() {
        final var emptyUnionType = new UnionType();
        final var unionSchema = mapper.toDataSchema(emptyUnionType);
        assertThat(unionSchema).isInstanceOf(UnionSchema.class);
        assertThat(((UnionSchema) unionSchema).members()).isEmpty();

        final var mappedBack = mapper.fromDataSchema(unionSchema);
        assertThat(mappedBack).isInstanceOf(UnionType.class);
        assertThat(((UnionType) mappedBack).members()).isEmpty();
    }

    @Test
    @DisplayName("toDataSchema throws SchemaException for unsupported/unknown custom type (eg. null)")
    void toDataSchemaUnsupportedTypeThrows() {
        assertThatThrownBy(() -> mapper.toDataSchema(null, null, (DataType) null))
                .isInstanceOf(SchemaException.class)
                .hasMessageContaining("Can not convert dataType");
    }
}
