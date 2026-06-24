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
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import io.axual.ksml.generator.YAMLObjectMapper;
import io.axual.ksml.runner.KSMLRunner;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
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
 * JSON Schema validation test for all ksml-runner.yaml configuration files under:
 * - ksml/src/test/resources/docs-examples
 * - ksml-integration-tests/src/test/resources/docs-examples
 * - examples/
 *
 * The schema is regenerated in-memory by invoking
 * {@code KSMLRunner.main("--runner-schema", tempFile)}, which is the same code path
 * Maven uses to produce the published {@code docs/ksml-runner-spec.json}. This guarantees
 * the test always runs against the latest spec derived from current source.
 */
@Slf4j
class AllRunnerConfigSchemaValidationTest {

    private static JsonSchema runnerSchema;

    static Stream<Path> provideRunnerYamlFiles() throws URISyntaxException, IOException {
        final var projectRoot = projectRoot();

        final var ksmlTestResourcesDir = projectRoot.resolve("ksml/src/test/resources/docs-examples");
        final var integrationTestsDir = projectRoot.resolve("ksml-integration-tests/src/test/resources/docs-examples");
        final var examplesDir = projectRoot.resolve("examples");

        return Stream.of(walkForRunnerYaml(ksmlTestResourcesDir),
                        walkForRunnerYaml(integrationTestsDir),
                        walkForRunnerYaml(examplesDir))
                .flatMap(s -> s)
                .sorted();
    }

    private static Path projectRoot() throws URISyntaxException {
        final var testResourcesUrl = AllRunnerConfigSchemaValidationTest.class.getResource("/");
        if (testResourcesUrl == null) {
            throw new IllegalStateException("Test resources directory not found");
        }
        // ksml-docs-test/target/test-classes -> ksml-docs-test/target -> ksml-docs-test -> project root
        return Paths.get(testResourcesUrl.toURI()).getParent().getParent().getParent();
    }

    private static Stream<Path> walkForRunnerYaml(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return Stream.empty();
        }
        return Files.walk(dir)
                .filter(Files::isRegularFile)
                .filter(path -> path.getFileName().toString().equals("ksml-runner.yaml"));
    }

    @BeforeAll
    static void generateSchema(@TempDir Path tempDir) throws Exception {
        final var schemaPath = tempDir.resolve("ksml-runner-spec.json");
        KSMLRunner.main(new String[]{"--runner-schema", schemaPath.toString()});

        if (!Files.exists(schemaPath)) {
            throw new IllegalStateException("Schema generation produced no file at: " + schemaPath);
        }

        final var schemaFactory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V201909);
        runnerSchema = schemaFactory.getSchema(Files.newInputStream(schemaPath));
        log.info("Generated and loaded runner schema from: {}", schemaPath);
    }

    @ParameterizedTest(name = "Validate {0} against generated ksml-runner schema")
    @MethodSource("provideRunnerYamlFiles")
    void validateRunnerYamlAgainstSchema(Path yamlFile) throws Exception {
        assertNotNull(runnerSchema, "Schema was not generated. Check generateSchema().");
        log.info("Validating: {}", yamlFile);

        final var yamlContent = Files.readString(yamlFile);
        final var jsonContent = YAMLObjectMapper.INSTANCE.readValue(yamlContent, JsonNode.class);

        final var violations = runnerSchema.validate(jsonContent);

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
