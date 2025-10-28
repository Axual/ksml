package io.axual.ksml.docs.examples;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2024 Axual B.V.
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
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.stream.Stream;

import io.axual.ksml.data.notation.json.JsonSchemaMapper;
import io.axual.ksml.definition.parser.TopologyDefinitionParser;
import io.axual.ksml.generator.YAMLObjectMapper;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * JSON Schema validation test for all YAML definition files
 * in docs-examples and pipelines folders from both the ksml module
 * and the ksml-integration-tests module.
 * The KSML JSON schema is generated dynamically using the same
 * TopologyDefinitionParser and JsonSchemaMapper that the KSML
 * runner uses when invoked with the --schema flag.
 */
public class AllDefinitionsSchemaValidationTest {

    private static JsonSchema ksmlSchema;

    /**
     * Discovers all YAML files in the resources directory from both the ksml module
     * and the ksml-integration-tests module.
     * Note: The Stream returned by this method is automatically closed by JUnit 5
     * after all parameterized tests complete. See JUnit 5 documentation:
     * <a href="https://junit.org/junit5/docs/current/user-guide/#writing-tests-parameterized-tests-argument-sources">...</a>
     */
    static Stream<Path> provideYamlFiles() throws URISyntaxException, IOException {
        // Get the directory containing the YAML files from ksml module
        var testResourcesUrl = AllDefinitionsSchemaValidationTest.class.getResource("/");
        if (testResourcesUrl == null) {
            throw new IllegalStateException("Test resources directory not found");
        }
        Path ksmlTestResourcesDir = Paths.get(testResourcesUrl.toURI());

        // Get the integration tests directory
        // From target/test-classes, go up 3 levels to reach project root, then navigate to integration tests
        Path integrationTestsDir = ksmlTestResourcesDir.getParent().getParent().getParent()
            .resolve("ksml-integration-tests/src/test/resources/docs-examples");

        // Find all .yaml files from both directories, excluding ksml-runner.yaml files (runner config, not KSML definitions)
        Stream<Path> ksmlFiles = Files.walk(ksmlTestResourcesDir)
            .filter(Files::isRegularFile)
            .filter(path -> path.toString().endsWith(".yaml"))
            .filter(path -> !path.getFileName().toString().equals("ksml-runner.yaml"));

        // Add integration tests files if the directory exists
        Stream<Path> integrationFiles = Stream.empty();
        if (Files.exists(integrationTestsDir)) {
            integrationFiles = Files.walk(integrationTestsDir)
                .filter(Files::isRegularFile)
                .filter(path -> path.toString().endsWith(".yaml"))
                .filter(path -> !path.getFileName().toString().equals("ksml-runner.yaml"));
        }

        // Combine both streams and sort for consistent test ordering
        return Stream.concat(ksmlFiles, integrationFiles).sorted();
    }

    /**
     * Generates the KSML JSON schema dynamically using TopologyDefinitionParser
     */
    @BeforeAll
    static void generateSchema() throws Exception {
        // Generate the schema dynamically using the same approach as KSML runner
        final var parser = new TopologyDefinitionParser("dummy");
        final var schemaJson = new JsonSchemaMapper(true).fromDataSchema(parser.schema());

        // Parse the schema JSON to add strict validation at root level
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode schemaNode = objectMapper.readTree(schemaJson);

        // Add "additionalProperties": false at root level to ensure strict validation
        // The generated schema by default allows additional properties, but for validation
        // we want to catch typos like "functionss:" instead of "functions:"
        ((ObjectNode) schemaNode).put("additionalProperties", false);

        // Load the schema using networknt validator with Draft 2019-09 support
        JsonSchemaFactory schemaFactory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V201909);
        ksmlSchema = schemaFactory.getSchema(schemaNode);
    }
    
    /**
     * Returns the generated KSML JSON schema
     */
    private static JsonSchema getKsmlSchema() {
        assertNotNull(ksmlSchema, "Schema was not generated. Run generateSchema() first.");
        return ksmlSchema;
    }

    @ParameterizedTest(name = "Validate {0} against KSML JSON Schema")
    @MethodSource("provideYamlFiles")
    void validateYamlFileAgainstSchema(Path yamlFile) throws Exception {
        System.out.println("Validating: " + yamlFile.getFileName());

        // Load the KSML schema
        JsonSchema schema = getKsmlSchema();

        // Read and parse the YAML file into JsonNode
        String yamlContent = Files.readString(yamlFile);
        JsonNode jsonContent = YAMLObjectMapper.INSTANCE.readValue(yamlContent, JsonNode.class);

        // Validate the JSON content against the schema
        Set<ValidationMessage> validationMessages = schema.validate(jsonContent);

        // Check if validation passed
        if (validationMessages.isEmpty()) {
            // Validation passed
            assertTrue(true, "YAML file " + yamlFile.getFileName() + " is valid against KSML schema");
        } else {
            // Validation failed - collect all error messages
            StringBuilder errorMessages = new StringBuilder();
            errorMessages.append("Schema validation failed for ").append(yamlFile.getFileName()).append(":\n");
            for (ValidationMessage msg : validationMessages) {
                errorMessages.append("  - ").append(msg.getMessage()).append("\n");
            }
            fail(errorMessages.toString());
        }
    }
}