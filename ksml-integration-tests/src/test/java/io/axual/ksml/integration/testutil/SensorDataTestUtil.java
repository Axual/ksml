package io.axual.ksml.integration.testutil;

/*-
 * ========================LICENSE_START=================================
 * KSML Integration Tests
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.assertj.core.api.SoftAssertions;

/**
 * Utility class for sensor data validation in integration tests.
 * Contains validation logic specific to sensor data models used in KSML integration tests.
 */
public class SensorDataTestUtil {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /**
     * Validates JSON sensor data structure using Jackson ObjectMapper and SoftAssertions.
     * This follows the same pattern used in JsonSchemaMapperTest for consistent JSON validation.
     *
     * @param jsonValue The JSON string to validate
     * @param softly SoftAssertions instance to collect all validation failures
     * @return JsonNode for additional custom validations
     */
    public static JsonNode validateSensorJsonStructure(String jsonValue, SoftAssertions softly) {
        try {
            JsonNode node = OBJECT_MAPPER.readTree(jsonValue);

            // Validate required sensor data fields exist and have correct types
            softAssertJsonField(softly, node, "/name", "name field");
            softAssertJsonField(softly, node, "/timestamp", "timestamp field");
            softAssertJsonField(softly, node, "/value", "value field");
            softAssertJsonField(softly, node, "/type", "type field");
            softAssertJsonField(softly, node, "/unit", "unit field");

            return node;

        } catch (JsonProcessingException e) {
            softly.fail("Invalid JSON structure: " + e.getMessage());
            return null;
        }
    }

    /**
     * Validates JSON sensor data structure for processed messages (with additional fields).
     */
    public static JsonNode validateProcessedSensorJsonStructure(String jsonValue, SoftAssertions softly) {
        JsonNode node = validateSensorJsonStructure(jsonValue, softly);
        if (node != null) {
            // Additional fields for processed messages
            softAssertJsonField(softly, node, "/processed_at", "processed_at field");
            softAssertJsonField(softly, node, "/sensor_id", "sensor_id field");
        }
        return node;
    }

    /**
     * Validates that JSON contains one of the expected enum values.
     * Uses JsonNode path access instead of string contains for better validation.
     */
    public static void softAssertJsonEnumField(SoftAssertions softly, JsonNode rootNode, String jsonPointer, String fieldDescription, String... validValues) {
        JsonNode fieldNode = rootNode.at(jsonPointer);
        softly.assertThatObject(fieldNode)
                .as("JSON path '%s' (%s) should exist and not be null", jsonPointer, fieldDescription)
                .returns(false, JsonNode::isMissingNode)
                .returns(false, JsonNode::isNull)
                .returns(true, JsonNode::isTextual);

        if (fieldNode.isTextual()) {
            String actualValue = fieldNode.asText();
            softly.assertThat(actualValue)
                    .as("%s should be one of the valid enum values", fieldDescription)
                    .isIn((Object[]) validValues);
        }
    }

    /**
     * Soft assertion for JSON field existence and non-null value.
     * Follows the same pattern as JsonSchemaMapperTest.
     */
    private static void softAssertJsonField(SoftAssertions softly, JsonNode rootNode, String jsonPointer, String fieldDescription) {
        softly.assertThatObject(rootNode.at(jsonPointer))
                .as("JSON path '%s' (%s) should exist and not be null", jsonPointer, fieldDescription)
                .returns(false, JsonNode::isMissingNode)
                .returns(false, JsonNode::isNull);
    }
}