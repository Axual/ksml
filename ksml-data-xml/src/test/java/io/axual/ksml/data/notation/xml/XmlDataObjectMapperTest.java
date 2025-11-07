package io.axual.ksml.data.notation.xml;

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

import io.axual.ksml.data.object.DataLong;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.type.StructType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.StringReader;

import static io.axual.ksml.data.schema.DataSchemaConstants.NO_TAG;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link XmlDataObjectMapper} verifying XML <-> DataObject conversions.
 *
 * <p>These tests ensure users can parse XML data with schemas and transform it in KSML pipelines.</p>
 */
@DisplayName("XmlDataObjectMapper - XML <-> DataObject conversions")
class XmlDataObjectMapperTest {

    private final XmlDataObjectMapper mapper = new XmlDataObjectMapper(false);
    private final XmlDataObjectMapper prettyMapper = new XmlDataObjectMapper(true);

    @Test
    @DisplayName("Converts XML to DataStruct with schema and back (round-trip)")
    void xmlToDataStructRoundTrip() {
        // Given: sensor data schema matching the docs example
        var schema = createSensorDataSchema();
        var structType = new StructType(schema);

        // And: XML data
        var xml = """
                <?xml version="1.0" encoding="UTF-8"?>
                <SensorData>
                    <name>sensor001</name>
                    <timestamp>1234567890</timestamp>
                    <value>23.5</value>
                    <type>temperature</type>
                    <unit>celsius</unit>
                    <color>red</color>
                    <city>Amsterdam</city>
                    <owner>alice</owner>
                </SensorData>
                """;

        // When: converting XML to DataStruct
        var dataObject = mapper.toDataObject(structType, xml);

        // Then: should create a DataStruct with all fields
        assertThat(dataObject).isInstanceOf(DataStruct.class);
        var struct = (DataStruct) dataObject;

        assertThat(struct.get("name").toString()).isEqualTo("sensor001");
        assertThat(struct.get("timestamp").toString()).isEqualTo("1234567890");
        assertThat(struct.get("value").toString()).isEqualTo("23.5");
        assertThat(struct.get("type").toString()).isEqualTo("temperature");
        assertThat(struct.get("unit").toString()).isEqualTo("celsius");
        assertThat(struct.get("color").toString()).isEqualTo("red");
        assertThat(struct.get("city").toString()).isEqualTo("Amsterdam");
        assertThat(struct.get("owner").toString()).isEqualTo("alice");

        // When: converting back to XML
        var resultXml = mapper.fromDataObject(struct);

        // Then: should produce valid, well-formed XML with all values
        assertThat(resultXml).isNotNull();

        try {
            // Parse the XML using DOM parser to verify it's valid and well-formed
            var doc = parseXmlDocument(resultXml);

            // Verify root element
            assertThat(doc.getDocumentElement().getTagName()).as("Root element should be SensorData").isEqualTo("SensorData");

            // Verify all fields are present and correct
            assertThat(getElementTextContent(doc, "name")).as("name element").isEqualTo("sensor001");
            assertThat(getElementTextContent(doc, "timestamp")).as("timestamp element").isEqualTo("1234567890");
            assertThat(getElementTextContent(doc, "value")).as("value element").isEqualTo("23.5");
            assertThat(getElementTextContent(doc, "type")).as("type element").isEqualTo("temperature");
            assertThat(getElementTextContent(doc, "unit")).as("unit element").isEqualTo("celsius");
            assertThat(getElementTextContent(doc, "color")).as("color element").isEqualTo("red");
            assertThat(getElementTextContent(doc, "city")).as("city element").isEqualTo("Amsterdam");
            assertThat(getElementTextContent(doc, "owner")).as("owner element").isEqualTo("alice");
        } catch (Exception e) {
            throw new AssertionError("Failed to parse XML result: " + resultXml, e);
        }
    }

    @Test
    @DisplayName("Pretty print mode formats XML with indentation")
    void prettyPrintMode() {
        // Given: a simple struct
        var schema = createSimpleSchema("name", "value");
        var struct = new DataStruct(schema);
        struct.put("name", DataString.from("test"));
        struct.put("value", DataString.from("123"));

        // When: converting with pretty print
        var prettyXml = prettyMapper.fromDataObject(struct);

        // Then: should contain newlines (indicating formatting)
        assertThat(prettyXml).contains("\n");
    }

    @Test
    @DisplayName("Compact mode produces single-line XML")
    void compactMode() {
        // Given: a simple struct
        var schema = createSimpleSchema("name", "value");
        var struct = new DataStruct(schema);
        struct.put("name", DataString.from("test"));
        struct.put("value", DataString.from("123"));

        // When: converting with compact mode (default)
        var compactXml = mapper.fromDataObject(struct);

        // Then: should be valid, well-formed single-line XML
        assertThat(compactXml).isNotNull();
        assertThat(compactXml.split("\n").length).as("Compact XML should be single line").isEqualTo(1);

        try {
            // Parse the XML using DOM parser to verify it's valid and well-formed
            var doc = parseXmlDocument(compactXml);

            // Verify root element
            assertThat(doc.getDocumentElement().getTagName()).as("Root element should be SimpleData").isEqualTo("SimpleData");

            // Verify all fields are present and correct
            assertThat(getElementTextContent(doc, "name")).as("name element").isEqualTo("test");
            assertThat(getElementTextContent(doc, "value")).as("value element").isEqualTo("123");
        } catch (Exception e) {
            throw new AssertionError("Failed to parse compact XML result: " + compactXml, e);
        }
    }

    @Test
    @DisplayName("Handles special XML characters correctly")
    void specialCharacterHandling() {
        // Given: schema and data with special XML characters
        var schema = createSimpleSchema("name", "description");
        var xml = """
                <?xml version="1.0" encoding="UTF-8"?>
                <SimpleData>
                    <name>test &amp; example</name>
                    <description>&lt;value&gt; with &quot;quotes&quot;</description>
                </SimpleData>
                """;

        // When: parsing XML
        var dataObject = mapper.toDataObject(new StructType(schema), xml);

        // Then: special characters should be decoded correctly
        assertThat(dataObject).isInstanceOf(DataStruct.class);
        var struct = (DataStruct) dataObject;

        // Verify exact decoded values after XML parsing
        assertThat(struct.get("name").toString()).isEqualTo("test & example");
        assertThat(struct.get("description").toString()).isEqualTo("<value> with \"quotes\"");
    }

    @Test
    @DisplayName("Round-trip preserves special characters with proper escaping")
    void specialCharactersRoundTrip() {
        // Given: struct with special characters
        var schema = createSimpleSchema("name", "description");
        var struct = new DataStruct(schema);
        struct.put("name", DataString.from("test & example"));
        struct.put("description", DataString.from("<value> with \"quotes\""));

        // When: round-trip
        var xml = mapper.fromDataObject(struct);
        var result = mapper.toDataObject(new StructType(schema), xml);

        // Then: values should be preserved
        assertThat(result).isInstanceOf(DataStruct.class);
        var resultStruct = (DataStruct) result;
        assertThat(resultStruct.get("name").toString()).isEqualTo("test & example");
        assertThat(resultStruct.get("description").toString()).contains("<value>");
        assertThat(resultStruct.get("description").toString()).contains("\"quotes\"");
    }

    @Test
    @DisplayName("Handles empty element values")
    void emptyElementHandling() {
        // Given: XML with empty element
        var schema = createSimpleSchema("name", "value");
        var xml = """
                <?xml version="1.0" encoding="UTF-8"?>
                <SimpleData>
                    <name>test</name>
                    <value></value>
                </SimpleData>
                """;

        // When: parsing
        var dataObject = mapper.toDataObject(new StructType(schema), xml);

        // Then: empty element should be represented
        assertThat(dataObject).isInstanceOf(DataStruct.class);
        var struct = (DataStruct) dataObject;
        assertThat(struct.get("name").toString()).isEqualTo("test");
        assertThat(struct.get("value").toString()).isEmpty();
    }

    @Test
    @DisplayName("Round-trip preserves field values and structure")
    void fieldValuePreservation() {
        // Given: sensor data struct
        var schema = createSensorDataSchema();
        var struct = new DataStruct(schema);
        struct.put("name", DataString.from("sensor001"));
        struct.put("timestamp", new DataLong(1234567890L));
        struct.put("value", DataString.from("23.5"));
        struct.put("type", DataString.from("temperature"));
        struct.put("unit", DataString.from("celsius"));
        struct.put("color", DataString.from("red"));
        struct.put("city", DataString.from("Amsterdam"));
        struct.put("owner", DataString.from("alice"));

        // When: round-trip conversion
        var xml = mapper.fromDataObject(struct);
        var result = mapper.toDataObject(new StructType(schema), xml);

        // Then: all fields should be preserved
        assertThat(result).isInstanceOf(DataStruct.class);
        var resultStruct = (DataStruct) result;

        assertThat(resultStruct.get("name").toString()).isEqualTo("sensor001");
        assertThat(resultStruct.get("timestamp").toString()).isEqualTo("1234567890");
        assertThat(resultStruct.get("value").toString()).isEqualTo("23.5");
        assertThat(resultStruct.get("type").toString()).isEqualTo("temperature");
        assertThat(resultStruct.get("unit").toString()).isEqualTo("celsius");
        assertThat(resultStruct.get("color").toString()).isEqualTo("red");
        assertThat(resultStruct.get("city").toString()).isEqualTo("Amsterdam");
        assertThat(resultStruct.get("owner").toString()).isEqualTo("alice");
    }

    @Test
    @DisplayName("Handles numeric types correctly")
    void numericTypeHandling() {
        // Given: schema with long type
        var fields = java.util.List.of(
                new StructSchema.Field("name", DataSchema.STRING_SCHEMA, "Name", NO_TAG, true, false, null),
                new StructSchema.Field("count", DataSchema.LONG_SCHEMA, "Count", NO_TAG, true, false, null)
        );
        var schema = new StructSchema("io.axual.test", "NumericData", "Numeric data", fields, false);

        // And: XML with numeric value
        var xml = """
                <?xml version="1.0" encoding="UTF-8"?>
                <NumericData>
                    <name>test</name>
                    <count>42</count>
                </NumericData>
                """;

        // When: parsing
        var dataObject = mapper.toDataObject(new StructType(schema), xml);

        // Then: numeric value should be parsed correctly
        assertThat(dataObject).isInstanceOf(DataStruct.class);
        var struct = (DataStruct) dataObject;
        assertThat(struct.get("name").toString()).isEqualTo("test");
        assertThat(struct.get("count").toString()).isEqualTo("42");
    }

    // Helper methods

    private StructSchema createSensorDataSchema() {
        var fields = java.util.List.of(
                new StructSchema.Field("name", DataSchema.STRING_SCHEMA, "Sensor name", NO_TAG, true, false, null),
                new StructSchema.Field("timestamp", DataSchema.LONG_SCHEMA, "Timestamp", NO_TAG, true, false, null),
                new StructSchema.Field("value", DataSchema.STRING_SCHEMA, "Sensor value", NO_TAG, true, false, null),
                new StructSchema.Field("type", DataSchema.STRING_SCHEMA, "Sensor type", NO_TAG, true, false, null),
                new StructSchema.Field("unit", DataSchema.STRING_SCHEMA, "Unit", NO_TAG, true, false, null),
                new StructSchema.Field("color", DataSchema.STRING_SCHEMA, "Color", NO_TAG, false, false, null),
                new StructSchema.Field("city", DataSchema.STRING_SCHEMA, "City", NO_TAG, false, false, null),
                new StructSchema.Field("owner", DataSchema.STRING_SCHEMA, "Owner", NO_TAG, false, false, null)
        );
        return new StructSchema("io.axual.test", "SensorData", "Sensor data schema", fields, false);
    }

    private StructSchema createSimpleSchema(String... fieldNames) {
        var fields = new java.util.ArrayList<StructSchema.Field>();
        for (var fieldName : fieldNames) {
            fields.add(new StructSchema.Field(fieldName, DataSchema.STRING_SCHEMA, fieldName, NO_TAG, true, false, null));
        }
        return new StructSchema("io.axual.test", "SimpleData", "Simple schema", fields, false);
    }

    /**
     * Parse an XML string into a DOM Document using standard Java XML parser.
     * This validates that the XML is well-formed and provides structured access to elements.
     *
     * @param xmlString The XML string to parse
     * @return Document object representing the parsed XML
     */
    private Document parseXmlDocument(String xmlString) throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        return builder.parse(new InputSource(new StringReader(xmlString)));
    }

    /**
     * Extract the text content of an XML element by tag name.
     *
     * @param doc     The XML document
     * @param tagName The tag name to extract
     * @return The text content of the element
     */
    private String getElementTextContent(Document doc, String tagName) {
        return doc.getElementsByTagName(tagName).item(0).getTextContent();
    }
}
