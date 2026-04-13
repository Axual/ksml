package io.axual.ksml.testrunner;

/*-
 * ========================LICENSE_START=================================
 * KSML Test Runner
 * %%
 * Copyright (C) 2021 - 2026 Axual B.V.
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
import com.google.common.collect.ImmutableMap;
import io.axual.ksml.TopologyGenerator;
import io.axual.ksml.definition.parser.TopologyDefinitionParser;
import io.axual.ksml.generator.YAMLObjectMapper;
import io.axual.ksml.parser.ParseNode;
import io.axual.ksml.python.PythonContextConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.graalvm.home.Version;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

/**
 * Standalone test runner for KSML pipeline definitions.
 * Parses YAML test definitions, sets up TopologyTestDriver, produces test data,
 * and runs Python assertions.
 */
@Slf4j
public class KSMLTestRunner {

    @CommandLine.Command(name = "ksml-test", description = "Run KSML pipeline tests defined in YAML")
    public static class Arguments {
        @CommandLine.Parameters(arity = "1..*", paramLabel = "PATH",
                description = "Test definition YAML files, or directories containing them (searched recursively)")
        List<File> testPaths;
    }

    public static void main(String[] args) {
        var arguments = new Arguments();
        var cmd = new CommandLine(arguments);
        try {
            cmd.parseArgs(args);
        } catch (CommandLine.ParameterException e) {
            cmd.usage(System.out);
            System.exit(1);
            return;
        }

        if (arguments.testPaths == null || arguments.testPaths.isEmpty()) {
            cmd.usage(System.out);
            System.exit(1);
            return;
        }

        // Check GraalVM availability
        if (!Version.getCurrent().isRelease()) {
            System.err.println("ERROR: KSML Test Runner requires GraalVM to run.");
            System.exit(1);
            return;
        }

        List<Path> testFiles;
        try {
            testFiles = collectTestFiles(arguments.testPaths);
        } catch (IOException | TestDefinitionException e) {
            System.err.println("ERROR: " + e.getMessage());
            System.exit(1);
            return;
        }

        if (testFiles.isEmpty()) {
            System.err.println("ERROR: No test files (*.yaml, *.yml) found in: " + arguments.testPaths);
            System.exit(1);
            return;
        }

        var runner = new KSMLTestRunner();
        var results = new ArrayList<TestResult>();
        for (var file : testFiles) {
            results.add(runner.runSingleTest(file));
        }

        // Report results
        reportResults(results);

        // Exit code
        boolean allPassed = results.stream().allMatch(r -> r.status() == TestResult.Status.PASS);
        System.exit(allPassed ? 0 : 1);
    }

    /**
     * Execute a single test definition and return the result.
     */
    public TestResult runSingleTest(Path testFile) {
        var parser = new TestDefinitionParser();
        var executionContext = new TestExecutionContext();
        TestDefinition definition;
        TopologyTestDriver driver = null;

        try {
            // 1. Parse test definition
            definition = parser.parse(testFile);
            log.info("Running test: {}", definition.name());

            // 2. Set up execution context
            executionContext.setup(resolveSchemaDirectory(testFile, definition.schemaDirectory()));

            // 3. Parse pipeline definition and build topology
            var topologyName = sanitizeTopologyName(definition.name());
            var yamlContent = readPipelineContent(testFile, definition.pipeline());
            var pipelineJson = YAMLObjectMapper.INSTANCE.readValue(yamlContent, JsonNode.class);
            var topologyDefinition = new TopologyDefinitionParser(topologyName)
                    .parse(ParseNode.fromRoot(pipelineJson, topologyName));

            var topologyGenerator = new TopologyGenerator(
                    topologyName + ".test",
                    null,
                    PythonContextConfig.builder().build()
            );

            var streamsBuilder = new StreamsBuilder();
            var topology = topologyGenerator.create(streamsBuilder,
                    ImmutableMap.of("definition", topologyDefinition));
            log.debug("Topology:\n{}", topology.describe());

            // 4. Create TopologyTestDriver
            driver = new TopologyTestDriver(topology);

            // 5. Produce test data
            var producer = new TestDataProducer(driver);
            producer.produce(definition.produce());

            // 6. Run assertions
            var assertionRunner = new AssertionRunner(driver);
            return assertionRunner.runAssertions(definition.assertions(), definition.name());

        } catch (TestDefinitionException e) {
            return TestResult.error(testFile.getFileName().toString(), "Invalid test definition: " + e.getMessage());
        } catch (Exception e) {
            var name = testFile.getFileName().toString();
            log.error("Error running test '{}'", name, e);
            return TestResult.error(name, e.getMessage());
        } finally {
            // 7. Cleanup
            if (driver != null) {
                driver.close();
            }
            executionContext.cleanup();
        }
    }

    private String resolveSchemaDirectory(Path testFile, String schemaDirectory) {
        if (schemaDirectory == null || schemaDirectory.isEmpty()) {
            return null;
        }
        // Try relative to test file first (most common for standalone use)
        var resolved = testFile.getParent().resolve(schemaDirectory);
        if (Files.isDirectory(resolved)) {
            return resolved.toAbsolutePath().toString();
        }
        // Try classpath (only works for exploded directories, not inside JARs)
        var url = getClass().getClassLoader().getResource(schemaDirectory);
        if (url != null && "file".equals(url.getProtocol())) {
            return url.getPath();
        }
        // Try as absolute path
        var absolute = Path.of(schemaDirectory);
        if (Files.isDirectory(absolute)) {
            return absolute.toAbsolutePath().toString();
        }
        return schemaDirectory;
    }

    private String readPipelineContent(Path testFile, String pipeline) throws java.io.IOException {
        // Try classpath first
        try (var stream = getClass().getClassLoader().getResourceAsStream(pipeline)) {
            if (stream != null) {
                return new String(stream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
            }
        }
        // Try relative to test file
        var resolved = testFile.getParent().resolve(pipeline);
        if (Files.exists(resolved)) {
            return Files.readString(resolved);
        }
        // Try as absolute path
        var absolute = Path.of(pipeline);
        if (Files.exists(absolute)) {
            return Files.readString(absolute);
        }
        throw new TestDefinitionException("Pipeline file not found: " + pipeline);
    }

    /**
     * Expand the user-supplied paths into a flat, sorted list of YAML test files.
     * Files are accepted as-is; directories are walked recursively for *.yaml / *.yml.
     */
    static List<Path> collectTestFiles(List<File> inputs) throws IOException {
        var files = new ArrayList<Path>();
        for (var input : inputs) {
            var path = input.toPath();
            if (Files.isDirectory(path)) {
                try (var stream = Files.walk(path)) {
                    stream.filter(Files::isRegularFile)
                            .filter(KSMLTestRunner::isYamlFile)
                            .forEach(files::add);
                }
            } else if (Files.isRegularFile(path)) {
                files.add(path);
            } else {
                throw new TestDefinitionException("Path not found: " + path);
            }
        }
        files.sort(Comparator.naturalOrder());
        return files;
    }

    private static boolean isYamlFile(Path path) {
        var name = path.getFileName().toString().toLowerCase(Locale.ROOT);
        return name.endsWith(".yaml") || name.endsWith(".yml");
    }

    /**
     * Sanitize a test name into a valid Kafka Streams topology name.
     * Kafka Streams only allows ASCII alphanumerics, '.', '_' and '-'.
     */
    private static String sanitizeTopologyName(String name) {
        return name.replaceAll("[^A-Za-z0-9._-]", "_");
    }

    private static void reportResults(List<TestResult> results) {
        System.out.println();
        System.out.println("=== KSML Test Results ===");
        System.out.println();

        int passed = 0;
        int failed = 0;
        int errored = 0;

        for (var result : results) {
            switch (result.status()) {
                case PASS -> {
                    System.out.println("  PASS  " + result.testName());
                    passed++;
                }
                case FAIL -> {
                    System.out.println("  FAIL  " + result.testName());
                    System.out.println("        " + result.message());
                    failed++;
                }
                case ERROR -> {
                    System.out.println("  ERROR " + result.testName());
                    System.out.println("        " + result.message());
                    errored++;
                }
            }
        }

        System.out.println();
        System.out.printf("%d passed, %d failed, %d errors%n", passed, failed, errored);
    }
}
