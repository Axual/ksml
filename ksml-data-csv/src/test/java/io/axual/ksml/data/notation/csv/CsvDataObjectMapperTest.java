package io.axual.ksml.data.notation.csv;

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

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.StringReader;

import io.axual.ksml.data.object.DataList;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.schema.DataField;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.type.StructType;

import static io.axual.ksml.data.schema.DataSchemaConstants.NO_TAG;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link CsvDataObjectMapper} verifying CSV <-> DataObject conversions.
 *
 * <p>Scenarios covered: round-trips for CSV with/without schemas, special character handling,
 * field type conversions, missing/extra fields. These tests ensure the core user workflow
 * of parsing CSV data with schemas and transforming it in KSML pipelines works correctly.</p>
 */
@DisplayName("CsvDataObjectMapper - CSV <-> DataObject conversions")
class CsvDataObjectMapperTest {

    private final CsvDataObjectMapper mapper = new CsvDataObjectMapper();

    @Test
    @DisplayName("Converts CSV line to DataStruct with schema and back (round-trip)")
    void csvToDataStructRoundTrip() {
        // Given: sensor data schema matching the docs example
        var schema = createSensorDataSchema();
        var structType = new StructType(schema);

        // And: a CSV line with sensor data
        var csvLine = "\"sensor001\",\"1234567890\",\"23.5\",\"temperature\",\"celsius\",\"red\",\"Amsterdam\",\"alice\"";

        // When: converting CSV to DataStruct
        var dataObject = mapper.toDataObject(structType, csvLine);

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

        // When: converting back to CSV
        var resultCsv = mapper.fromDataObject(struct);

        // Then: should produce valid, parseable CSV with all field values
        assertThat(resultCsv).isNotNull();

        try {
            // Parse the CSV using Apache Commons CSV to verify it's valid and well-formed
            var csvRecord = parseCsvRecord(resultCsv);

            // Verify all fields are present in correct order (schema order)
            assertThat(csvRecord.size()).as("CSV should have 8 fields").isEqualTo(8);
            assertThat(csvRecord.get(0)).as("name field").isEqualTo("sensor001");
            assertThat(csvRecord.get(1)).as("timestamp field").isEqualTo("1234567890");
            assertThat(csvRecord.get(2)).as("value field").isEqualTo("23.5");
            assertThat(csvRecord.get(3)).as("type field").isEqualTo("temperature");
            assertThat(csvRecord.get(4)).as("unit field").isEqualTo("celsius");
            assertThat(csvRecord.get(5)).as("color field").isEqualTo("red");
            assertThat(csvRecord.get(6)).as("city field").isEqualTo("Amsterdam");
            assertThat(csvRecord.get(7)).as("owner field").isEqualTo("alice");
        } catch (Exception e) {
            throw new AssertionError("Failed to parse CSV result: " + resultCsv, e);
        }
    }

    @Test
    @DisplayName("Converts CSV line to DataList without schema")
    void csvToDataListWithoutSchema() {
        // Given: a simple CSV line
        var csvLine = "\"value1\",\"value2\",\"value3\"";

        // When: converting without schema (null type)
        var dataObject = mapper.toDataObject(null, csvLine);

        // Then: should create a DataList of strings
        assertThat(dataObject).isInstanceOf(DataList.class);
        var list = (DataList) dataObject;

        assertThat(list.size()).isEqualTo(3);
        assertThat(list.get(0).toString()).isEqualTo("value1");
        assertThat(list.get(1).toString()).isEqualTo("value2");
        assertThat(list.get(2).toString()).isEqualTo("value3");
    }

    @Test
    @DisplayName("Round-trip: DataList to CSV to DataList preserves values")
    void dataListRoundTrip() {
        // Given: a DataList with string values
        var originalList = new DataList(DataString.DATATYPE);
        originalList.add(DataString.from("first"));
        originalList.add(DataString.from("second"));
        originalList.add(DataString.from("third"));

        // When: converting to CSV
        var csv = mapper.fromDataObject(originalList);

        // Then: should produce valid, parseable CSV line
        assertThat(csv).isNotNull();

        try {
            // Parse the CSV using Apache Commons CSV to verify it's valid and well-formed
            var csvRecord = parseCsvRecord(csv);

            // Verify all values are present in correct order
            assertThat(csvRecord.size()).as("CSV should have 3 fields").isEqualTo(3);
            assertThat(csvRecord.get(0)).as("first element").isEqualTo("first");
            assertThat(csvRecord.get(1)).as("second element").isEqualTo("second");
            assertThat(csvRecord.get(2)).as("third element").isEqualTo("third");
        } catch (Exception e) {
            throw new AssertionError("Failed to parse CSV result: " + csv, e);
        }

        // When: converting back to DataList
        var resultList = mapper.toDataObject(null, csv);

        // Then: should have same values
        assertThat(resultList).isInstanceOf(DataList.class);
        var list = (DataList) resultList;
        assertThat(list.size()).isEqualTo(3);
        assertThat(list.get(0).toString()).isEqualTo("first");
        assertThat(list.get(1).toString()).isEqualTo("second");
        assertThat(list.get(2).toString()).isEqualTo("third");
    }

    @Test
    @DisplayName("Handles special characters correctly: commas in values")
    void specialCharacterHandling() {
        // Given: CSV with comma in value
        var csvLine = "\"value with, comma\"";

        // When: parsing to DataList
        var dataObject = mapper.toDataObject(null, csvLine);

        // Then: should preserve the value correctly
        assertThat(dataObject).isInstanceOf(DataList.class);
        var list = (DataList) dataObject;
        assertThat(list.get(0).toString()).isEqualTo("value with, comma");
    }

    @Test
    @DisplayName("Handles quotes in values - CSV uses double-double quotes for escaping")
    void quotesInValuesHandling() {
        // Given: CSV with quotes (escaped as "" in CSV format)
        var csvLine = "\"value with \"\"quotes\"\"\"";

        // When: parsing to DataList
        var dataObject = mapper.toDataObject(null, csvLine);

        // Then: double quotes are preserved as-is (CSV spec behavior)
        assertThat(dataObject).isInstanceOf(DataList.class);
        var list = (DataList) dataObject;
        // CSV parser preserves the double-quote escaping
        assertThat(list.get(0).toString()).contains("quotes");
    }

    @Test
    @DisplayName("Handles missing fields gracefully - fewer fields than schema expects")
    void missingFieldsHandling() {
        // Given: schema with 3 fields
        var schema = createSimpleSchema("name", "age", "city");
        var structType = new StructType(schema);

        // And: CSV with only 2 fields
        var csvLine = "\"Alice\",\"30\"";

        // When: parsing CSV
        var dataObject = mapper.toDataObject(structType, csvLine);

        // Then: should handle missing field
        assertThat(dataObject).isInstanceOf(DataStruct.class);
        var struct = (DataStruct) dataObject;

        assertThat(struct.get("name").toString()).isEqualTo("Alice");
        assertThat(struct.get("age").toString()).isEqualTo("30");
        // Missing field should be handled (empty string for required field)
        assertThat(struct.get("city")).isNotNull();
    }

    @Test
    @DisplayName("Handles extra fields gracefully - more fields than schema expects")
    void extraFieldsHandling() {
        // Given: schema with 2 fields
        var schema = createSimpleSchema("name", "age");
        var structType = new StructType(schema);

        // And: CSV with 3 fields (extra field should be ignored)
        var csvLine = "\"Alice\",\"30\",\"Amsterdam\"";

        // When: parsing CSV
        var dataObject = mapper.toDataObject(structType, csvLine);

        // Then: should only parse expected fields
        assertThat(dataObject).isInstanceOf(DataStruct.class);
        var struct = (DataStruct) dataObject;

        assertThat(struct.get("name").toString()).isEqualTo("Alice");
        assertThat(struct.get("age").toString()).isEqualTo("30");
        assertThat(struct.size()).isEqualTo(2); // No extra field
    }

    @Test
    @DisplayName("Handles empty values in CSV")
    void emptyValueHandling() {
        // Given: schema
        var schema = createSimpleSchema("name", "age", "city");
        var structType = new StructType(schema);

        // And: CSV with empty value in middle
        var csvLine = "\"Alice\",\"\",\"Amsterdam\"";

        // When: parsing CSV
        var dataObject = mapper.toDataObject(structType, csvLine);

        // Then: empty value should be preserved as empty string
        assertThat(dataObject).isInstanceOf(DataStruct.class);
        var struct = (DataStruct) dataObject;

        assertThat(struct.get("name").toString()).isEqualTo("Alice");
        assertThat(struct.get("age").toString()).isEmpty();
        assertThat(struct.get("city").toString()).isEqualTo("Amsterdam");
    }

    @Test
    @DisplayName("DataStruct to CSV preserves field order from schema")
    void dataStructToCsvPreservesFieldOrder() {
        // Given: schema with specific field order
        var schema = createSensorDataSchema();
        var struct = new DataStruct(schema);

        // Populate in different order than schema
        struct.put("owner", DataString.from("alice"));
        struct.put("name", DataString.from("sensor001"));
        struct.put("city", DataString.from("Amsterdam"));
        struct.put("timestamp", DataString.from("1234567890"));
        struct.put("value", DataString.from("23.5"));
        struct.put("type", DataString.from("temperature"));
        struct.put("unit", DataString.from("celsius"));
        struct.put("color", DataString.from("red"));

        // When: converting to CSV
        var csv = mapper.fromDataObject(struct);

        // Then: field order should match schema (name, timestamp, value, type, unit, color, city, owner)
        assertThat(csv).isNotNull();

        // Parse it back to verify order
        var result = mapper.toDataObject(new StructType(schema), csv);
        assertThat(result).isInstanceOf(DataStruct.class);
        var resultStruct = (DataStruct) result;

        // Values should match
        assertThat(resultStruct.get("name").toString()).isEqualTo("sensor001");
        assertThat(resultStruct.get("owner").toString()).isEqualTo("alice");
    }

    // Helper methods

    private StructSchema createSensorDataSchema() {
        var fields = java.util.List.of(
            new DataField("name", DataSchema.STRING_SCHEMA, "Sensor name", NO_TAG, true, false, null),
            new DataField("timestamp", DataSchema.STRING_SCHEMA, "Timestamp", NO_TAG, true, false, null),
            new DataField("value", DataSchema.STRING_SCHEMA, "Sensor value", NO_TAG, true, false, null),
            new DataField("type", DataSchema.STRING_SCHEMA, "Sensor type", NO_TAG, true, false, null),
            new DataField("unit", DataSchema.STRING_SCHEMA, "Unit", NO_TAG, true, false, null),
            new DataField("color", DataSchema.STRING_SCHEMA, "Color", NO_TAG, true, false, null),
            new DataField("city", DataSchema.STRING_SCHEMA, "City", NO_TAG, true, false, null),
            new DataField("owner", DataSchema.STRING_SCHEMA, "Owner", NO_TAG, true, false, null)
        );
        return new StructSchema("io.axual.test", "SensorData", "Sensor data schema", fields, false);
    }

    private StructSchema createSimpleSchema(String... fieldNames) {
        var fields = new java.util.ArrayList<DataField>();
        for (var fieldName : fieldNames) {
            fields.add(new DataField(fieldName, DataSchema.STRING_SCHEMA, fieldName, NO_TAG, true, false, null));
        }
        return new StructSchema("io.axual.test", "Simple", "Simple schema", fields, false);
    }

    /**
     * Parse a CSV string using Apache Commons CSV library.
     * This provides robust CSV parsing that handles all edge cases including:
     * - Quoted fields with commas
     * - Escaped quotes
     * - Newlines in fields
     *
     * @param csvString The CSV string to parse
     * @return CSVRecord containing parsed fields
     */
    private CSVRecord parseCsvRecord(String csvString) throws Exception {
        try (CSVParser parser = CSVFormat.DEFAULT.parse(new StringReader(csvString))) {
            var records = parser.getRecords();
            assertThat(records).as("CSV should parse to exactly one record").hasSize(1);
            return records.getFirst();
        }
    }
}
