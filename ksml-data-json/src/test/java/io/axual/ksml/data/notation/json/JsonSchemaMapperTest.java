package io.axual.ksml.data.notation.json;

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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.schema.EnumSchema;
import io.axual.ksml.data.schema.ListSchema;
import io.axual.ksml.data.schema.MapSchema;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.schema.UnionSchema;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static io.axual.ksml.data.schema.DataSchema.ANY_SCHEMA;
import static io.axual.ksml.data.schema.DataSchema.BOOLEAN_SCHEMA;
import static io.axual.ksml.data.schema.DataSchema.BYTE_SCHEMA;
import static io.axual.ksml.data.schema.DataSchema.DOUBLE_SCHEMA;
import static io.axual.ksml.data.schema.DataSchema.FLOAT_SCHEMA;
import static io.axual.ksml.data.schema.DataSchema.INTEGER_SCHEMA;
import static io.axual.ksml.data.schema.DataSchema.LONG_SCHEMA;
import static io.axual.ksml.data.schema.DataSchema.NULL_SCHEMA;
import static io.axual.ksml.data.schema.DataSchema.SHORT_SCHEMA;
import static io.axual.ksml.data.schema.DataSchema.STRING_SCHEMA;
import static io.axual.ksml.data.schema.DataSchemaConstants.NO_TAG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatObject;

/**
 * Tests for {@link JsonSchemaMapper} validating JSON Schema <-> KSML DataSchema conversions.
 *
 * <p>These tests use AssertJ chained assertions and SoftAssertions to verify multiple
 * properties in one go, similar to other tests in this module. Representative
 * JSON Schema samples are parsed into StructSchema/DataSchema and converted back
 * to JSON to assert round-trip fidelity.</p>
 */
@DisplayName("JsonSchemaMapper - JSON Schema <-> KSML DataSchema")
class JsonSchemaMapperTest {
    private static final ObjectMapper JACKSON = new ObjectMapper();

    private final JsonSchemaMapper mapper = new JsonSchemaMapper(false);

    @Test
    @DisplayName("Parses object with primitive fields and maps to correct DataSchemas and back")
    void parseObjectWithPrimitives() throws Exception {
        final var json = readResource("/jsonschema/object_with_primitives.json");

        final var struct = assertThat(mapper.toDataSchema("ns", "ObjWithPrims", json))
                .asInstanceOf(InstanceOfAssertFactories.type(StructSchema.class))
                .returns("ObjWithPrims", StructSchema::name)
                .returns(true, StructSchema::additionalFieldsAllowed)
                .returns(ANY_SCHEMA, StructSchema::additionalFieldsSchema)
                .actual();

        assertThat(struct.fields())
                .hasSize(5)
                .containsExactlyInAnyOrder(

                        new StructSchema.Field("aString", STRING_SCHEMA, null, NO_TAG, false),
                        new StructSchema.Field("aBoolean", BOOLEAN_SCHEMA, null, NO_TAG, false),
                        new StructSchema.Field("anInteger", LONG_SCHEMA, null, NO_TAG, false),
                        new StructSchema.Field("aNumber", DOUBLE_SCHEMA, null, NO_TAG, false),
                        new StructSchema.Field("aNull", NULL_SCHEMA, null, NO_TAG, false));

        // Verify conversion back to JsonSchema
        final var jsonString = assertThat(mapper.fromDataSchema(struct))
                .isNotBlank()
                .actual();

        final var rootNode = assertThatObject(JACKSON.readTree(jsonString))
                .returns(true, JsonNode::isObject)
                .asInstanceOf(InstanceOfAssertFactories.type(ObjectNode.class))
                .actual();
        // Use soft assertions to get feedback on multiple field issues at once
        final var softly = new SoftAssertions();
        softly.assertThat(rootNode)
                .hasSize(6);  // Updated from 5 to 6 to account for $schema field
        softAssertJsonStringField(softly, rootNode, "/$schema", "https://json-schema.org/draft/2019-09/schema");
        softAssertJsonStringField(softly, rootNode, "/title", "ObjWithPrims");
        softAssertJsonStringField(softly, rootNode, "/description", "Example object with primitive fields");
        softAssertJsonStringField(softly, rootNode, "/type", "object");

        softly.assertThatObject(rootNode.at("/properties"))
                .returns(false, JsonNode::isMissingNode)
                .returns(true, JsonNode::isObject)
                .returns(5, JsonNode::size);

        softAssertJsonStringField(softly, rootNode, "/properties/aString/type", "string");
        softAssertJsonStringField(softly, rootNode, "/properties/aBoolean/type", "boolean");
        softAssertJsonStringField(softly, rootNode, "/properties/anInteger/type", "integer");
        softAssertJsonStringField(softly, rootNode, "/properties/aNumber/type", "number");
        softAssertJsonStringField(softly, rootNode, "/properties/aNull/type", "null");

        softly.assertAll();
    }

    @Test
    @DisplayName("Parses object with inner objects to handle additionalProperties")
    void parseAdditionalProperties() throws Exception {
        final var json = readResource("/jsonschema/object_additional_properties.json");

        final var dataSchema = assertThat(mapper.toDataSchema("ns", "AdditionalProps", json))
                .as("The schema must be AdditionalProps with the correct documentation and allow any additional field")
                .isNotNull()
                .asInstanceOf(InstanceOfAssertFactories.type(StructSchema.class))
                .returns("AdditionalProps", StructSchema::name)
                .returns("Test object to verify that additional properties are accepted", StructSchema::doc)
                .returns(true, StructSchema::additionalFieldsAllowed)
                .returns(ANY_SCHEMA, StructSchema::additionalFieldsSchema)
                .actual();
        final var fields = assertThat(dataSchema.fields())
                .as("The schema must have four fields")
                .hasSize(4)
                .actual();

        // Verify the anyAdditional field
        var anyAdditionalFieldSchema = assertThat(fields)
                .as("The first field must be the anyAdditional field, not required and have a struct schema")
                .first()
                .isNotNull()
                .returns("anyAdditional", StructSchema.Field::name)
                .returns(false, StructSchema.Field::required)
                .returns(false, StructSchema.Field::constant)
                .extracting(StructSchema.Field::schema)
                .isNotNull()
                .asInstanceOf(InstanceOfAssertFactories.type(StructSchema.class))
                .actual();
        assertThat(anyAdditionalFieldSchema)
                .as("The anyAdditional field schema must have Additional Fields of type any")
                .returns("ObjectWithAnyAdditional", StructSchema::name)
                .returns(true, StructSchema::additionalFieldsAllowed)
                .extracting(StructSchema::additionalFieldsSchema)
                .isEqualTo(ANY_SCHEMA);
        assertInnerFieldXExists(anyAdditionalFieldSchema);

        // Verify the noAdditional field
        var noAdditionalFieldSchema = assertThat(fields.get(1))
                .as("The second field must be the noAdditional field, not required and have a struct schema")
                .isNotNull()
                .returns("noAdditional", StructSchema.Field::name)
                .returns(false, StructSchema.Field::required)
                .returns(false, StructSchema.Field::constant)
                .extracting(StructSchema.Field::schema)
                .isNotNull()
                .asInstanceOf(InstanceOfAssertFactories.type(StructSchema.class))
                .actual();
        assertThat(noAdditionalFieldSchema)
                .as("The noAdditional field schema must have Additional Fields disabled")
                .returns("ObjectWithNoAdditional", StructSchema::name)
                .returns(false, StructSchema::additionalFieldsAllowed)
                .extracting(StructSchema::additionalFieldsSchema)
                .isNull();
        assertInnerFieldXExists(noAdditionalFieldSchema);

        // Verify the objectAdditional field
        var objectAdditionalFieldSchema = assertThat(fields.get(2))
                .as("The third field must be the objectAdditional field, not required and have a struct schema")
                .isNotNull()
                .returns("objectAdditional", StructSchema.Field::name)
                .returns(false, StructSchema.Field::required)
                .returns(false, StructSchema.Field::constant)
                .extracting(StructSchema.Field::schema)
                .isNotNull()
                .asInstanceOf(InstanceOfAssertFactories.type(StructSchema.class))
                .actual();
        var innerObjectSchema = assertThat(objectAdditionalFieldSchema)
                .as("The stringAdditional field schema must have additionalFieldsSchema of type String")
                .returns("ObjectWithSimpleInnerAdditional", StructSchema::name)
                .returns(true, StructSchema::additionalFieldsAllowed)
                .extracting(StructSchema::additionalFieldsSchema)
                .asInstanceOf(InstanceOfAssertFactories.type(StructSchema.class))
                .returns("ReallySimpleInner", StructSchema::name)
                .actual();
        assertInnerFieldXExists(innerObjectSchema);
        assertInnerFieldXExists(objectAdditionalFieldSchema);

        // Verify the stringAdditional field
        var stringAdditionalFieldSchema = assertThat(fields.get(3))
                .as("The third field must be the stringAdditional field, not required and have a struct schema")
                .isNotNull()
                .returns("stringAdditional", StructSchema.Field::name)
                .returns(false, StructSchema.Field::required)
                .returns(false, StructSchema.Field::constant)
                .extracting(StructSchema.Field::schema)
                .isNotNull()
                .asInstanceOf(InstanceOfAssertFactories.type(StructSchema.class))
                .actual();
        assertThat(stringAdditionalFieldSchema)
                .as("The stringAdditional field schema must have Additional Fields of type String")
                .returns("ObjectWithStringAdditional", StructSchema::name)
                .returns(true, StructSchema::additionalFieldsAllowed)
                .extracting(StructSchema::additionalFieldsSchema)
                .isEqualTo(STRING_SCHEMA);
        assertInnerFieldXExists(stringAdditionalFieldSchema);

        // Verify conversion back to JsonSchema
        final var jsonString = assertThat(mapper.fromDataSchema(dataSchema))
                .isNotBlank()
                .actual();

        final var rootNode = assertThatObject(JACKSON.readTree(jsonString))
                .returns(true, JsonNode::isObject)
                .asInstanceOf(InstanceOfAssertFactories.type(ObjectNode.class))
                .actual();
        // Use soft assertions to get feedback on multiple field issues at once
        final var softly = new SoftAssertions();
        softly.assertThat(rootNode)
                .hasSize(7);  // Updated from 6 to 7 to account for $schema field
        softAssertJsonStringField(softly, rootNode, "/$schema", "https://json-schema.org/draft/2019-09/schema");
        softAssertJsonStringField(softly, rootNode, "/title", "AdditionalProps");
        softAssertJsonStringField(softly, rootNode, "/description", "Test object to verify that additional properties are accepted");
        softAssertJsonStringField(softly, rootNode, "/type", "object");
        softAssertJsonBooleanField(softly, rootNode, "/additionalProperties", true);
        softly.assertThatObject(rootNode.at("/properties"))
                .returns(false, JsonNode::isMissingNode)
                .returns(true, JsonNode::isObject)
                .extracting(JsonNode::properties, InstanceOfAssertFactories.set(Map.Entry.class))
                .extracting(Map.Entry::getKey)
                .containsExactlyInAnyOrder("anyAdditional", "noAdditional", "stringAdditional", "objectAdditional");
        softAssertJsonStringField(softly, rootNode, "/properties/anyAdditional/type", "object");
        softAssertJsonStringField(softly, rootNode, "/properties/anyAdditional/$ref", "#/$defs/ObjectWithAnyAdditional");
        softAssertJsonStringField(softly, rootNode, "/properties/noAdditional/type", "object");
        softAssertJsonStringField(softly, rootNode, "/properties/noAdditional/$ref", "#/$defs/ObjectWithNoAdditional");
        softAssertJsonStringField(softly, rootNode, "/properties/stringAdditional/type", "object");
        softAssertJsonStringField(softly, rootNode, "/properties/stringAdditional/$ref", "#/$defs/ObjectWithStringAdditional");
        softAssertJsonStringField(softly, rootNode, "/properties/objectAdditional/type", "object");
        softAssertJsonStringField(softly, rootNode, "/properties/objectAdditional/$ref", "#/$defs/ObjectWithSimpleInnerAdditional");

        softly.assertThatObject(rootNode.at("/$defs"))
                .returns(false, JsonNode::isMissingNode)
                .returns(true, JsonNode::isObject)
                .extracting(JsonNode::properties, InstanceOfAssertFactories.set(Map.Entry.class))
                .extracting(Map.Entry::getKey)
                .containsExactlyInAnyOrder(
                        "ObjectWithAnyAdditional",
                        "ObjectWithNoAdditional",
                        "ObjectWithSimpleInnerAdditional",
                        "ObjectWithStringAdditional",
                        "ReallySimpleInner");

        // Test the defined records, using full paths to leaves instead of objects check
        softAssertJsonStringField(softly, rootNode, "/$defs/ReallySimpleInner/type", "object");
        softAssertJsonStringField(softly, rootNode, "/$defs/ReallySimpleInner/title", "ReallySimpleInner");
        softAssertJsonStringField(softly, rootNode, "/$defs/ReallySimpleInner/properties/x/type", "string");
        softAssertRequiredXOnly(softly, rootNode, "/$defs/ReallySimpleInner/required");
        softAssertJsonBooleanField(softly, rootNode, "/$defs/ReallySimpleInner/additionalProperties", true);

        softAssertJsonStringField(softly, rootNode, "/$defs/ObjectWithAnyAdditional/type", "object");
        softAssertJsonStringField(softly, rootNode, "/$defs/ObjectWithAnyAdditional/title", "ObjectWithAnyAdditional");
        softAssertJsonStringField(softly, rootNode, "/$defs/ObjectWithAnyAdditional/properties/x/type", "string");
        softAssertRequiredXOnly(softly, rootNode, "/$defs/ObjectWithAnyAdditional/required");
        softAssertJsonBooleanField(softly, rootNode, "/$defs/ObjectWithAnyAdditional/additionalProperties", true);

        softAssertJsonStringField(softly, rootNode, "/$defs/ObjectWithNoAdditional/type", "object");
        softAssertJsonStringField(softly, rootNode, "/$defs/ObjectWithNoAdditional/title", "ObjectWithNoAdditional");
        softAssertJsonStringField(softly, rootNode, "/$defs/ObjectWithNoAdditional/properties/x/type", "string");
        softAssertRequiredXOnly(softly, rootNode, "/$defs/ObjectWithNoAdditional/required");
        softAssertJsonBooleanField(softly, rootNode, "/$defs/ObjectWithNoAdditional/additionalProperties", false);

        softAssertJsonStringField(softly, rootNode, "/$defs/ObjectWithStringAdditional/type", "object");
        softAssertJsonStringField(softly, rootNode, "/$defs/ObjectWithStringAdditional/title", "ObjectWithStringAdditional");
        softAssertJsonStringField(softly, rootNode, "/$defs/ObjectWithStringAdditional/properties/x/type", "string");
        softAssertRequiredXOnly(softly, rootNode, "/$defs/ObjectWithStringAdditional/required");
        softAssertJsonStringField(softly, rootNode, "/$defs/ObjectWithStringAdditional/additionalProperties/type", "string");

        softAssertJsonStringField(softly, rootNode, "/$defs/ObjectWithSimpleInnerAdditional/type", "object");
        softAssertJsonStringField(softly, rootNode, "/$defs/ObjectWithSimpleInnerAdditional/title", "ObjectWithSimpleInnerAdditional");
        softAssertJsonStringField(softly, rootNode, "/$defs/ObjectWithSimpleInnerAdditional/properties/x/type", "string");
        softAssertRequiredXOnly(softly, rootNode, "/$defs/ObjectWithSimpleInnerAdditional/required");
        softAssertJsonStringField(softly, rootNode, "/$defs/ObjectWithSimpleInnerAdditional/additionalProperties/type", "object");
        softAssertJsonStringField(softly, rootNode, "/$defs/ObjectWithSimpleInnerAdditional/additionalProperties/$ref", "#/$defs/ReallySimpleInner");

        softly.assertAll();
    }

    @Test
    @DisplayName("Parses required fields, arrays, enums, unions, refs, and complex arrays")
    void parseComplexFeatures() throws Exception {
        final var json = readResource("/jsonschema/objects_with_complex_features.json");


        final var struct = assertThat(mapper.toDataSchema("ns", "ComplexFeatures", json))
                .asInstanceOf(InstanceOfAssertFactories.type(StructSchema.class))
                .returns("ComplexFeatures", StructSchema::name)
                .returns("Example object with more complex fields", StructSchema::doc)
                .returns(true, StructSchema::additionalFieldsAllowed)
                .returns(ANY_SCHEMA, StructSchema::additionalFieldsSchema)
                .actual();

        final var softlyDataSchema = new SoftAssertions();
        softlyDataSchema.assertThat(struct.fields())
                .hasSize(12);

        // Required String field
        softlyDataSchema.assertThat(struct.field("reqStr"))
                .isEqualTo(new StructSchema.Field("reqStr", STRING_SCHEMA, null, NO_TAG, true));

        // Primitive checks
        softlyDataSchema.assertThat(struct.field("str"))
                .isEqualTo(new StructSchema.Field("str", STRING_SCHEMA, null, NO_TAG, false));
        softlyDataSchema.assertThat(struct.field("bool"))
                .isEqualTo(new StructSchema.Field("bool", BOOLEAN_SCHEMA, null, NO_TAG, false));
        softlyDataSchema.assertThat(struct.field("int"))
                .isEqualTo(new StructSchema.Field("int", LONG_SCHEMA, null, NO_TAG, false));
        softlyDataSchema.assertThat(struct.field("num"))
                .isEqualTo(new StructSchema.Field("num", DOUBLE_SCHEMA, null, NO_TAG, false));
        softlyDataSchema.assertThat(struct.field("nullField"))
                .isEqualTo(new StructSchema.Field("nullField", NULL_SCHEMA, null, NO_TAG, false));

        // Arrays
        softlyDataSchema.assertThat(struct.field("arrayAny"))
                .isEqualTo(new StructSchema.Field("arrayAny", new ListSchema(ANY_SCHEMA), null, NO_TAG, false));
        softlyDataSchema.assertThat(struct.field("arrayString"))
                .isEqualTo(new StructSchema.Field("arrayString", new ListSchema(STRING_SCHEMA), null, NO_TAG, false));

        // Enum
        assertThat(struct.field("color"))
                .isEqualTo(new StructSchema.Field("color",
                        new EnumSchema(List.of(
                                EnumSchema.Symbol.of("RED"),
                                EnumSchema.Symbol.of("GREEN"),
                                EnumSchema.Symbol.of("BLUE"))
                        ), null, NO_TAG, false));

        // Union anyOf
        softlyDataSchema.assertThat(struct.field("idUnion"))
                .isEqualTo(new StructSchema.Field("idUnion", new UnionSchema(
                        new UnionSchema.Member(LONG_SCHEMA),
                        new UnionSchema.Member(STRING_SCHEMA)
                ), null, NO_TAG, false));

        // $ref to internal definition
        softlyDataSchema.assertThat(struct.field("innerRef"))
                .isEqualTo(new StructSchema.Field("innerRef", new StructSchema(List.of(
                        new StructSchema.Field("x", STRING_SCHEMA, null, NO_TAG, true),
                        new StructSchema.Field("y", LONG_SCHEMA, null, NO_TAG, false)
                )), null, NO_TAG, false));

        // Complex array items: union of enum and object
        softlyDataSchema.assertThat(struct.field("arrComplex"))
                .isEqualTo(new StructSchema.Field("arrComplex", new ListSchema(
                        new UnionSchema(
                                new UnionSchema.Member(new EnumSchema(List.of(EnumSchema.Symbol.of("A"), EnumSchema.Symbol.of("B")))),
                                new UnionSchema.Member(new StructSchema(List.of(
                                        new StructSchema.Field("v", DOUBLE_SCHEMA, null, NO_TAG, false)
                                )))
                        )
                ), null, NO_TAG, false));

        softlyDataSchema.assertAll();

        // Verify conversion back to JsonSchema
        final var jsonString = assertThat(mapper.fromDataSchema(struct))
                .isNotBlank()
                .actual();

        final var rootNode = assertThatObject(JACKSON.readTree(jsonString))
                .returns(true, JsonNode::isObject)
                .asInstanceOf(InstanceOfAssertFactories.type(ObjectNode.class))
                .actual();

        // Use soft assertions to get feedback on multiple field issues at once
        final var softlyJson = new SoftAssertions();
        softlyJson.assertThat(rootNode)
                .hasSize(8);  // Updated from 7 to 8 to account for $schema field
        // Assert the root object data
        softAssertJsonStringField(softlyJson, rootNode, "/$schema", "https://json-schema.org/draft/2019-09/schema");
        softAssertJsonStringField(softlyJson, rootNode, "/title", "ComplexFeatures");
        softAssertJsonStringField(softlyJson, rootNode, "/description", "Example object with more complex fields");
        softAssertJsonStringField(softlyJson, rootNode, "/type", "object");
        softAssertJsonBooleanField(softlyJson, rootNode, "/additionalProperties", true);
        softlyJson.assertThatObject(rootNode.at("/properties"))
                .returns(false, JsonNode::isMissingNode)
                .returns(true, JsonNode::isObject)
                .extracting(JsonNode::properties, InstanceOfAssertFactories.set(Map.Entry.class))
                .extracting(Map.Entry::getKey)
                .containsExactlyInAnyOrder("str", "bool", "int", "num", "nullField", "reqStr", "arrayAny", "arrayString", "color", "idUnion", "innerRef", "arrComplex");
        softlyJson.assertThatObject(rootNode.at("/required"))
                .returns(false, JsonNode::isMissingNode)
                .returns(true, JsonNode::isArray)
                .asInstanceOf(InstanceOfAssertFactories.type(ArrayNode.class))
                .returns(1, ArrayNode::size)
                .extracting(node -> node.get(0))
                .isNotNull()
                .returns(true, JsonNode::isTextual)
                .returns("reqStr", JsonNode::asText);

        // Assert the single expected object definition
        softAssertJsonStringField(softlyJson, rootNode, "/$defs/AnonymousStructSchema/title", "AnonymousStructSchema");
        softAssertJsonStringField(softlyJson, rootNode, "/$defs/AnonymousStructSchema/type", "object");
        softAssertJsonBooleanField(softlyJson, rootNode, "/$defs/AnonymousStructSchema/additionalProperties", true);
        softlyJson.assertThatObject(rootNode.at("/$defs/AnonymousStructSchema/properties"))
                .returns(false, JsonNode::isMissingNode)
                .returns(true, JsonNode::isObject)
                .asInstanceOf(InstanceOfAssertFactories.type(ObjectNode.class))
                .returns(1, ObjectNode::size);
        softAssertJsonStringField(softlyJson, rootNode, "/$defs/AnonymousStructSchema/properties/v/type", "number");

        // Verify properties arrComplex
        softAssertJsonStringField(softlyJson, rootNode, "/properties/arrComplex/type", "array");
        softlyJson.assertThatObject(rootNode.at("/properties/arrComplex/items/anyOf"))
                .returns(false, JsonNode::isMissingNode)
                .returns(true, JsonNode::isArray)
                .asInstanceOf(InstanceOfAssertFactories.type(ArrayNode.class))
                .returns(2, ArrayNode::size);
        // Verify properties arrComplex enum A and B array
        softlyJson.assertThatObject(rootNode.at("/properties/arrComplex/items/anyOf/0/enum"))
                .returns(false, JsonNode::isMissingNode)
                .returns(true, JsonNode::isArray)
                .asInstanceOf(InstanceOfAssertFactories.type(ArrayNode.class))
                .returns(2, ArrayNode::size);
        softAssertJsonStringField(softlyJson, rootNode, "/properties/arrComplex/items/anyOf/0/enum/0", "A");
        softAssertJsonStringField(softlyJson, rootNode, "/properties/arrComplex/items/anyOf/0/enum/1", "B");

        // Verify properties arrComplex anonymous struct schema
        softAssertJsonStringField(softlyJson, rootNode, "/properties/arrComplex/items/anyOf/1/type", "object");
        softAssertJsonStringField(softlyJson, rootNode, "/properties/arrComplex/items/anyOf/1/$ref", "#/$defs/AnonymousStructSchema");

        // Verify properties arrayAny
        softAssertJsonStringField(softlyJson, rootNode, "/properties/arrayAny/type", "array");
        softlyJson.assertThatObject(rootNode.at("/properties/arrayAny/items"))
                .as("arrayAny should not exist")
                .returns(true, JsonNode::isMissingNode);

        // Verify properties arrayString
        softAssertJsonStringField(softlyJson, rootNode, "/properties/arrayString/type", "array");
        softAssertJsonStringField(softlyJson, rootNode, "/properties/arrayString/items/type", "string");

        // Verify properties color enum
        softlyJson.assertThatObject(rootNode.at("/properties/color/enum"))
                .returns(false, JsonNode::isMissingNode)
                .returns(true, JsonNode::isArray)
                .asInstanceOf(InstanceOfAssertFactories.type(ArrayNode.class))
                .returns(3, ArrayNode::size);
        softAssertJsonStringField(softlyJson, rootNode, "/properties/color/enum/0", "RED");
        softAssertJsonStringField(softlyJson, rootNode, "/properties/color/enum/1", "GREEN");
        softAssertJsonStringField(softlyJson, rootNode, "/properties/color/enum/2", "BLUE");

        // Verify properties idUnion union
        softlyJson.assertThatObject(rootNode.at("/properties/idUnion/anyOf"))
                .returns(false, JsonNode::isMissingNode)
                .returns(true, JsonNode::isArray)
                .asInstanceOf(InstanceOfAssertFactories.type(ArrayNode.class))
                .returns(2, ArrayNode::size);
        softAssertJsonStringField(softlyJson, rootNode, "/properties/idUnion/anyOf/0/type", "integer");
        softAssertJsonStringField(softlyJson, rootNode, "/properties/idUnion/anyOf/1/type", "string");

        // Verify properties bool/type
        softAssertJsonStringField(softlyJson, rootNode, "/properties/bool/type", "boolean");
        // Verify properties innerRef reference object
        softAssertJsonStringField(softlyJson, rootNode, "/properties/innerRef/type", "object");
        softAssertJsonStringField(softlyJson, rootNode, "/properties/innerRef/$ref", "#/$defs/AnonymousStructSchema");
        // Verify properties int type
        softAssertJsonStringField(softlyJson, rootNode, "/properties/int/type", "integer");
        // Verify properties nullField type
        softAssertJsonStringField(softlyJson, rootNode, "/properties/nullField/type", "null");
        // Verify properties num type
        softAssertJsonStringField(softlyJson, rootNode, "/properties/num/type", "number");
        // Verify properties reqStr type
        softAssertJsonStringField(softlyJson, rootNode, "/properties/reqStr/type", "string");
        // Verify properties str type
        softAssertJsonStringField(softlyJson, rootNode, "/properties/str/type", "string");

        softlyJson.assertAll();
    }

    @Test
    @DisplayName("Map complex DataStruct definitions like $ref, anyOf, enum (for constant), arrays")
    void convertCustomDataStructToJson() throws Exception {
        // Create a child struct to be referenced
        final var childStruct = StructSchema.builder().namespace("ns").name("Child").doc("child doc")
                .field(new StructSchema.Field("innerString", STRING_SCHEMA, "string doc", NO_TAG, true))
                .field(new StructSchema.Field("innerBoolean", BOOLEAN_SCHEMA, "boolean doc", NO_TAG, true))
                .field(new StructSchema.Field("innerInteger", INTEGER_SCHEMA, "integer doc", NO_TAG, true))
                .field(new StructSchema.Field("innerLong", LONG_SCHEMA, "long doc", NO_TAG, true))
                .field(new StructSchema.Field("innerShort", SHORT_SCHEMA, "short doc", NO_TAG, true))
                .field(new StructSchema.Field("innerFloat", FLOAT_SCHEMA, "float doc", NO_TAG, true))
                .field(new StructSchema.Field("innerDouble", DOUBLE_SCHEMA, "double doc", NO_TAG, true))
                .field(new StructSchema.Field("innerByte", BYTE_SCHEMA, "byte doc", NO_TAG, true))
                .build();

        final var mapOfStrings = new MapSchema(STRING_SCHEMA);

        // Create union for id
        final var idUnion = new UnionSchema(new UnionSchema.Member(LONG_SCHEMA), new UnionSchema.Member(STRING_SCHEMA));

        // Constant string field encoded as enum single value on JSON side
        final var constCode = new StructSchema.Field("constCode", STRING_SCHEMA, "Constant Code", NO_TAG, true, true, new DataString("X"));

        final var colorSchema = new EnumSchema("", "Color", "", List.of(new EnumSchema.Symbol("RED"), new EnumSchema.Symbol("GREEN"), new EnumSchema.Symbol("BLUE")), new EnumSchema.Symbol("GREEN"));
        final var color = new StructSchema.Field("color", colorSchema, "The color", NO_TAG, true, true, new DataString("RED"));

        final var topStruct = StructSchema.builder().namespace("ns").name("Top").doc("top doc")
                .field(new StructSchema.Field("name", STRING_SCHEMA, "name doc", NO_TAG, true))
                .field(new StructSchema.Field("id", idUnion, "id union", NO_TAG, true))
                .field(new StructSchema.Field("tags", new ListSchema(STRING_SCHEMA), "tags", NO_TAG, false))
                .field(new StructSchema.Field("child", childStruct, "child ref", NO_TAG, false))
                .field(new StructSchema.Field("mapped", mapOfStrings, "map of string", NO_TAG, true))
                .field(color)
                .field(constCode)
                .additionalFieldsAllowed(false)
                .build();

        final var json = assertThat(mapper.fromDataSchema(topStruct))
                .isNotBlank()
                .actual();
        // Parse JSON output to validate structure
        final var rootNode = assertThat(JACKSON.readTree(json))
                .isNotNull()
                .asInstanceOf(InstanceOfAssertFactories.type(ObjectNode.class))
                .actual();
        final var softlyJson = new SoftAssertions();
        softAssertJsonStringField(softlyJson, rootNode, "/type", "object");
        softAssertJsonStringField(softlyJson, rootNode, "/title", "Top");
        softAssertJsonStringField(softlyJson, rootNode, "/description", "top doc");
        softAssertJsonBooleanField(softlyJson, rootNode, "/additionalProperties", false);
        softlyJson.assertThatObject(rootNode.at("/required"))
                .returns(false, JsonNode::isMissingNode)
                .returns(true, JsonNode::isArray)
                .asInstanceOf(InstanceOfAssertFactories.type(ArrayNode.class))
                .extracting(ArrayNode::valueStream, InstanceOfAssertFactories.stream(JsonNode.class))
                .extracting(JsonNode::asText)
                .containsExactlyInAnyOrderElementsOf(List.of("color", "constCode", "name", "id", "mapped"));

        softlyJson.assertThatObject(rootNode.at("/properties"))
                .returns(false, JsonNode::isMissingNode)
                .returns(true, JsonNode::isObject)
                .extracting(JsonNode::properties, InstanceOfAssertFactories.set(Map.Entry.class))
                .extracting(Map.Entry::getKey)
                .containsExactlyInAnyOrder("name", "id", "tags", "child", "mapped", "color", "constCode");

        // Verify the union id
        softAssertJsonStringField(softlyJson, rootNode, "/properties/id/description", "id union");
        softlyJson.assertThatObject(rootNode.at("/properties/id/anyOf"))
                .returns(false, JsonNode::isMissingNode)
                .returns(true, JsonNode::isArray)
                .asInstanceOf(InstanceOfAssertFactories.type(ArrayNode.class))
                .returns(2, ArrayNode::size)
        ;
        softAssertJsonStringField(softlyJson, rootNode, "/properties/id/anyOf/0/type", "integer");
        softAssertJsonStringField(softlyJson, rootNode, "/properties/id/anyOf/1/type", "string");

        // Verify that the internal object is a reference
        softAssertJsonStringField(softlyJson, rootNode, "/properties/child/description", "child ref");
        softAssertJsonStringField(softlyJson, rootNode, "/properties/child/$ref", "#/$defs/Child");
        softAssertJsonStringField(softlyJson, rootNode, "/properties/child/type", "object");
        // Verify the actual child reference
        softAssertJsonStringField(softlyJson, rootNode, "/$defs/Child/type", "object");
        softAssertJsonStringField(softlyJson, rootNode, "/$defs/Child/title", "Child");
        softAssertJsonStringField(softlyJson, rootNode, "/$defs/Child/description", "child doc");
        softAssertJsonBooleanField(softlyJson, rootNode, "/$defs/Child/additionalProperties", true);
        softlyJson.assertThatObject(rootNode.at("/$defs/Child/properties"))
                .returns(false, JsonNode::isMissingNode)
                .returns(true, JsonNode::isObject)
                .extracting(JsonNode::properties, InstanceOfAssertFactories.set(Map.Entry.class))
                .extracting(Map.Entry::getKey)
                .containsExactlyInAnyOrder("innerBoolean", "innerByte", "innerDouble", "innerFloat", "innerInteger", "innerLong", "innerShort", "innerString");
        softlyJson.assertThatObject(rootNode.at("/$defs/Child/required"))
                .returns(false, JsonNode::isMissingNode)
                .returns(true, JsonNode::isArray)
                .asInstanceOf(InstanceOfAssertFactories.type(ArrayNode.class))
                .extracting(ArrayNode::valueStream, InstanceOfAssertFactories.stream(JsonNode.class))
                .extracting(JsonNode::asText)
                .containsExactlyInAnyOrder("innerBoolean", "innerByte", "innerDouble", "innerFloat", "innerInteger", "innerLong", "innerShort", "innerString");
        softAssertJsonStringField(softlyJson, rootNode, "/$defs/Child/properties/innerBoolean/type", "boolean");
        softAssertJsonStringField(softlyJson, rootNode, "/$defs/Child/properties/innerBoolean/description", "boolean doc");
        softAssertJsonStringField(softlyJson, rootNode, "/$defs/Child/properties/innerByte/type", "integer");
        softAssertJsonStringField(softlyJson, rootNode, "/$defs/Child/properties/innerByte/description", "byte doc");
        softAssertJsonStringField(softlyJson, rootNode, "/$defs/Child/properties/innerDouble/type", "number");
        softAssertJsonStringField(softlyJson, rootNode, "/$defs/Child/properties/innerDouble/description", "double doc");
        softAssertJsonStringField(softlyJson, rootNode, "/$defs/Child/properties/innerFloat/type", "number");
        softAssertJsonStringField(softlyJson, rootNode, "/$defs/Child/properties/innerFloat/description", "float doc");
        softAssertJsonStringField(softlyJson, rootNode, "/$defs/Child/properties/innerInteger/type", "integer");
        softAssertJsonStringField(softlyJson, rootNode, "/$defs/Child/properties/innerInteger/description", "integer doc");
        softAssertJsonStringField(softlyJson, rootNode, "/$defs/Child/properties/innerLong/type", "integer");
        softAssertJsonStringField(softlyJson, rootNode, "/$defs/Child/properties/innerLong/description", "long doc");
        softAssertJsonStringField(softlyJson, rootNode, "/$defs/Child/properties/innerShort/type", "integer");
        softAssertJsonStringField(softlyJson, rootNode, "/$defs/Child/properties/innerShort/description", "short doc");
        softAssertJsonStringField(softlyJson, rootNode, "/$defs/Child/properties/innerString/type", "string");
        softAssertJsonStringField(softlyJson, rootNode, "/$defs/Child/properties/innerString/description", "string doc");

        // Verify constCode
        softAssertJsonStringField(softlyJson, rootNode, "/properties/constCode/description", "Constant Code");
        softlyJson.assertThatObject(rootNode.at("/properties/constCode/enum"))
                .returns(false, JsonNode::isMissingNode)
                .returns(true, JsonNode::isArray)
                .extracting(n -> n.get(0))
                .extracting(JsonNode::asText)
                .isEqualTo("X");

        // Verify properties color enum
        softAssertJsonStringField(softlyJson, rootNode, "/properties/color/description", "The color");
        softlyJson.assertThatObject(rootNode.at("/properties/color/enum"))
                .returns(false, JsonNode::isMissingNode)
                .returns(true, JsonNode::isArray)
                .asInstanceOf(InstanceOfAssertFactories.type(ArrayNode.class))
                .returns(3, ArrayNode::size);
        softAssertJsonStringField(softlyJson, rootNode, "/properties/color/enum/0", "RED");
        softAssertJsonStringField(softlyJson, rootNode, "/properties/color/enum/1", "GREEN");
        softAssertJsonStringField(softlyJson, rootNode, "/properties/color/enum/2", "BLUE");

        // Verify the mapped property
        softAssertJsonStringField(softlyJson, rootNode, "/properties/mapped/description", "map of string");
        softAssertJsonStringField(softlyJson, rootNode, "/properties/mapped/type", "object");
        softlyJson.assertThatObject(rootNode.at("/properties/mapped/additionalProperties"))
                .returns(false, JsonNode::isMissingNode)
                .returns(true, JsonNode::isObject)
                .returns(1, JsonNode::size);
        softAssertJsonStringField(softlyJson, rootNode, "/properties/mapped/additionalProperties/type", "string");

        // Verify the name property
        softAssertJsonStringField(softlyJson, rootNode, "/properties/name/description", "name doc");
        softAssertJsonStringField(softlyJson, rootNode, "/properties/name/type", "string");

        // Verify the tags property
        softAssertJsonStringField(softlyJson, rootNode, "/properties/tags/description", "tags");
        softAssertJsonStringField(softlyJson, rootNode, "/properties/tags/type", "array");
        softlyJson.assertThatObject(rootNode.at("/properties/tags/items"))
                .returns(false, JsonNode::isMissingNode)
                .returns(true, JsonNode::isObject)
                .returns(1, JsonNode::size);
        softAssertJsonStringField(softlyJson, rootNode, "/properties/tags/items/type", "string");

        softlyJson.assertAll();

    }

    private static void softAssertRequiredXOnly(final SoftAssertions softly, final ObjectNode rootNode, final String requiredPath) {
        softly.assertThatObject(rootNode.at(requiredPath))
                .returns(false, JsonNode::isMissingNode)
                .returns(true, JsonNode::isArray)
                .asInstanceOf(InstanceOfAssertFactories.type(ArrayNode.class))
                .returns(1, ArrayNode::size)
                .extracting(node -> node.get(0))
                .isNotNull()
                .returns(true, JsonNode::isTextual)
                .returns("x", JsonNode::asText);
    }

    private static void softAssertJsonStringField(final SoftAssertions softly, final ObjectNode rootNode, final String jsonPointer, final String expectedValue) {
        softly.assertThatObject(rootNode.at(jsonPointer))
                .as("Path '%s' did not match expected value '%s'", jsonPointer, expectedValue)
                .returns(false, JsonNode::isMissingNode)
                .returns(true, JsonNode::isTextual)
                .extracting(JsonNode::asText)
                .isEqualTo(expectedValue);
    }

    private static void softAssertJsonBooleanField(final SoftAssertions softly, final ObjectNode rootNode, final String jsonPointer, final boolean expectedValue) {
        softly.assertThatObject(rootNode.at(jsonPointer))
                .as("Path '%s' did not match expected value '%s'", jsonPointer, expectedValue)
                .returns(false, JsonNode::isMissingNode)
                .returns(true, JsonNode::isBoolean)
                .extracting(JsonNode::asBoolean)
                .isEqualTo(expectedValue);
    }

    void assertInnerFieldXExists(StructSchema structSchema) {
        assertThat(structSchema.fields()).hasSize(1)
                .as("Schema %s must have required string field named 'x'", structSchema.name())
                .first()
                .isNotNull()
                .isEqualTo(new StructSchema.Field("x", STRING_SCHEMA, null, NO_TAG, true));

    }

    @Test
    @DisplayName("JSON Schema spec compliance: missing additionalProperties defaults to true")
    void testJsonSchemaSpecComplianceForMissingAdditionalProperties() {
        // Test that missing additionalProperties defaults to true (per JSON Schema spec)
        String jsonSchema = """
                {
                  "type": "object",
                  "properties": {
                    "name": { "type": "string" }
                  }
                }
                """;

        var schema = mapper.toDataSchema("ns", "Test", jsonSchema);
        assertThat(schema).isInstanceOf(StructSchema.class);
        var structSchema = (StructSchema) schema;

        assertThat(structSchema.additionalFieldsAllowed())
                .as("Missing additionalProperties should default to true per JSON Schema spec")
                .isTrue();
        assertThat(structSchema.additionalFieldsSchema())
                .as("Default additionalProperties should allow any type")
                .isEqualTo(ANY_SCHEMA);
    }

    @Test
    @DisplayName("Explicit additionalProperties: false is respected")
    void testExplicitAdditionalPropertiesFalse() {
        String jsonSchema = """
                {
                  "type": "object",
                  "properties": {
                    "name": { "type": "string" }
                  },
                  "additionalProperties": false
                }
                """;

        var schema = mapper.toDataSchema("ns", "Test", jsonSchema);
        var structSchema = (StructSchema) schema;

        assertThat(structSchema.additionalFieldsAllowed()).isFalse();
        assertThat(structSchema.additionalFieldsSchema()).isNull();
    }

    @Test
    @DisplayName("Explicit additionalProperties: true is respected")
    void testExplicitAdditionalPropertiesTrue() {
        String jsonSchema = """
                {
                  "type": "object",
                  "properties": {
                    "name": { "type": "string" }
                  },
                  "additionalProperties": true
                }
                """;

        var schema = mapper.toDataSchema("ns", "Test", jsonSchema);
        var structSchema = (StructSchema) schema;

        assertThat(structSchema.additionalFieldsAllowed()).isTrue();
        assertThat(structSchema.additionalFieldsSchema()).isEqualTo(ANY_SCHEMA);
    }

    @Test
    @DisplayName("Typed additionalProperties schema is correctly parsed")
    void testTypedAdditionalProperties() {
        String jsonSchema = """
                {
                  "type": "object",
                  "properties": {
                    "name": { "type": "string" }
                  },
                  "additionalProperties": { "type": "number" }
                }
                """;

        var schema = mapper.toDataSchema("ns", "Test", jsonSchema);
        var structSchema = (StructSchema) schema;

        assertThat(structSchema.additionalFieldsAllowed()).isTrue();
        assertThat(structSchema.additionalFieldsSchema()).isEqualTo(DOUBLE_SCHEMA);
    }

    @Test
    @DisplayName("Complex additionalProperties with object schema")
    void testComplexAdditionalProperties() {
        String jsonSchema = """
                {
                  "type": "object",
                  "properties": {
                    "name": { "type": "string" }
                  },
                  "additionalProperties": {
                    "type": "object",
                    "properties": {
                      "value": { "type": "string" }
                    },
                    "required": ["value"]
                  }
                }
                """;

        var schema = mapper.toDataSchema("ns", "Test", jsonSchema);
        var structSchema = (StructSchema) schema;

        assertThat(structSchema.additionalFieldsAllowed()).isTrue();
        assertThat(structSchema.additionalFieldsSchema())
                .isInstanceOf(StructSchema.class);

        var additionalSchema = (StructSchema) structSchema.additionalFieldsSchema();
        assertThat(additionalSchema.fields()).hasSize(1);
        assertThat(additionalSchema.field("value"))
                .isNotNull()
                .returns("value", StructSchema.Field::name)
                .returns(true, StructSchema.Field::required)
                .returns(STRING_SCHEMA, StructSchema.Field::schema);
    }

    private static String readResource(String path) throws Exception {
        try (var is = JsonSchemaMapperTest.class.getResourceAsStream(path)) {
            if (is == null) throw new IllegalArgumentException("Resource not found: " + path);
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
