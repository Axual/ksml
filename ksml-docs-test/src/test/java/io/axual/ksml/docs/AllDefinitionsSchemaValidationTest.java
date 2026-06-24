package io.axual.ksml.docs;

/*-
 * ========================LICENSE_START=================================
 * KSML Docs Test
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
import io.axual.ksml.data.notation.json.JsonSchemaMapper;
import io.axual.ksml.definition.parser.TopologyDefinitionParser;
import io.axual.ksml.generator.YAMLObjectMapper;
import lombok.extern.slf4j.Slf4j;
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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * JSON Schema validation test for all KSML definition YAML files (any *.yaml that is not
 * a ksml-runner.yaml) under:
 * - ksml/src/test/resources
 * - ksml-integration-tests/src/test/resources/docs-examples
 *
 * The KSML JSON schema is generated dynamically using the same TopologyDefinitionParser
 * and JsonSchemaMapper that KSML uses when invoked with the --schema flag, so the test
 * always runs against the latest spec derived from current source.
 */
@Slf4j
class AllDefinitionsSchemaValidationTest {

    private static JsonSchema ksmlSchema;

    static Stream<Path> provideYamlFiles() throws URISyntaxException, IOException {
        final var projectRoot = projectRoot();

        final var ksmlTestResourcesDir = projectRoot.resolve("ksml/src/test/resources");
        final var integrationTestsDir = projectRoot.resolve("ksml-integration-tests/src/test/resources/docs-examples");

        return Stream.of(walkForDefinitionYaml(ksmlTestResourcesDir),
                        walkForDefinitionYaml(integrationTestsDir))
                .flatMap(s -> s)
                .sorted();
    }

    private static Path projectRoot() throws URISyntaxException {
        final var testResourcesUrl = AllDefinitionsSchemaValidationTest.class.getResource("/");
        if (testResourcesUrl == null) {
            throw new IllegalStateException("Test resources directory not found");
        }
        // ksml-docs-test/target/test-classes -> ksml-docs-test/target -> ksml-docs-test -> project root
        return Paths.get(testResourcesUrl.toURI()).getParent().getParent().getParent();
    }

    private static Stream<Path> walkForDefinitionYaml(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return Stream.empty();
        }
        return Files.walk(dir)
                .filter(Files::isRegularFile)
                .filter(path -> path.toString().endsWith(".yaml"))
                .filter(path -> !path.getFileName().toString().equals("ksml-runner.yaml"));
    }

    @BeforeAll
    static void generateSchema() throws Exception {
        final var parser = new TopologyDefinitionParser("dummy");
        final var schemaJson = new JsonSchemaMapper(true).fromDataSchema(parser.schema());

        final var objectMapper = new ObjectMapper();
        final var schemaNode = objectMapper.readTree(schemaJson);
        // The generated schema permits additional properties by default; tighten this at the root so
        // we catch top-level typos like "functionss:" instead of "functions:".
        ((ObjectNode) schemaNode).put("additionalProperties", false);

        final var schemaFactory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V201909);
        ksmlSchema = schemaFactory.getSchema(schemaNode);
    }

    @ParameterizedTest(name = "Validate {0} against KSML JSON Schema")
    @MethodSource("provideYamlFiles")
    void validateYamlFileAgainstSchema(Path yamlFile) throws Exception {
        assertNotNull(ksmlSchema, "Schema was not generated. Check generateSchema().");
        log.info("Validating: {}", yamlFile);

        final var yamlContent = Files.readString(yamlFile);
        final var jsonContent = YAMLObjectMapper.INSTANCE.readValue(yamlContent, JsonNode.class);

        final var violations = ksmlSchema.validate(jsonContent);

        if (!violations.isEmpty()) {
            final var errorMessages = new StringBuilder();
            errorMessages.append("Schema validation failed for ").append(yamlFile).append(":\n");
            for (final var msg : violations) {
                errorMessages.append("  - ").append(msg.getMessage()).append("\n");
            }
            fail(errorMessages.toString());
        }
    }
}
