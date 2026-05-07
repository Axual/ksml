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
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Standalone test runner for KSML pipeline tests defined in YAML.
 * Each YAML file is a test suite (one or more named tests sharing a pipeline);
 * the runner produces one {@link TestResult} per test in each suite.
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
            results.addAll(runner.runTestFile(file));
        }

        // Report results
        reportResults(results);

        // Exit code
        boolean allPassed = results.stream().allMatch(r -> r.status() == TestResult.Status.PASS);
        System.exit(allPassed ? 0 : 1);
    }

    /**
     * Execute every test in a suite file and return one {@link TestResult} per test.
     * If the suite YAML or the referenced definition YAML cannot be parsed, returns a single ERROR result.
     */
    public List<TestResult> runTestFile(Path testFile) {
        var parser = new TestDefinitionParser();
        var suiteFallbackName = filenameWithoutExtension(testFile);

        TestSuiteDefinition suite;
        String definitionYaml;
        try {
            suite = parser.parse(testFile);
            definitionYaml = readDefinitionContent(testFile, suite.definition());
        } catch (TestDefinitionException e) {
            return List.of(TestResult.error(suiteFallbackName, "<parse>",
                    "Invalid test definition: " + e.getMessage()));
        } catch (Exception e) {
            return List.of(TestResult.error(suiteFallbackName, "<parse>", describeFailure(e)));
        }

        var suiteName = suite.name() != null && !suite.name().isEmpty() ? suite.name() : suiteFallbackName;
        var topicTypeMap = TestDefinitionParser.buildTopicTypeMap(suite);
        var schemaDirectory = resolveDirectory(testFile, suite.schemaDirectory());
        var modulesDirectory = resolveDirectory(testFile, suite.moduleDirectory());

        log.info("Running suite: {} ({} test(s))", suiteName, suite.tests().size());

        var results = new ArrayList<TestResult>();
        for (var entry : suite.tests().entrySet()) {
            var testKey = entry.getKey();
            var testCase = entry.getValue();
            var displayLabel = testCase.description() != null && !testCase.description().isEmpty()
                    ? testCase.description() : testKey;
            results.add(runSingleTest(suite, suiteName, testKey, displayLabel,
                    testCase, definitionYaml, topicTypeMap, schemaDirectory, modulesDirectory));
        }
        return results;
    }

    private TestResult runSingleTest(TestSuiteDefinition suite, String suiteName,
                                     String testKey, String displayLabel, TestCaseDefinition testCase,
                                     String definitionYaml, Map<String, StreamDefinition> topicTypeMap,
                                     String schemaDirectory, String modulesDirectory) {
        var executionContext = new TestExecutionContext();
        TopologyTestDriver driver = null;
        try {
            log.info("  Running test: {} › {}", suiteName, displayLabel);

            executionContext.setup(schemaDirectory, topicTypeMap);

            var topologyName = sanitizeTopologyName(suiteName + "_" + testKey);
            var definitionJson = YAMLObjectMapper.INSTANCE.readValue(definitionYaml, JsonNode.class);
            var topologyDefinition = new TopologyDefinitionParser(topologyName)
                    .parse(ParseNode.fromRoot(definitionJson, topologyName));

            var topologyGenerator = new TopologyGenerator(
                    topologyName + ".test",
                    null,
                    PythonContextConfig.builder()
                            .modulePath(modulesDirectory)
                            .build()
            );

            var streamsBuilder = new StreamsBuilder();
            var topology = topologyGenerator.create(streamsBuilder,
                    ImmutableMap.of("definition", topologyDefinition));
            log.debug("Topology:\n{}", topology.describe());

            driver = new TopologyTestDriver(topology);

            var producer = new TestDataProducer(driver, suite.streams());
            producer.produce(testCase.produce());

            var assertionRunner = new AssertionRunner(driver, suite.streams());
            return assertionRunner.runAssertions(testCase.assertions(), suiteName, displayLabel);

        } catch (TestDefinitionException e) {
            return TestResult.error(suiteName, displayLabel,
                    "Invalid test definition: " + e.getMessage());
        } catch (Exception e) {
            // Log at debug — the exception detail is preserved in the returned TestResult.message
            // via describeFailure(e), so reportResults() shows it on the ERROR line for CLI users.
            log.debug("Error running test '{} › {}'", suiteName, displayLabel, e);
            return TestResult.error(suiteName, displayLabel, describeFailure(e));
        } finally {
            if (driver != null) {
                driver.close();
            }
            executionContext.cleanup();
        }
    }

    /**
     * Resolve a directory path as specified in the definition to an absolute path.
     */
    private String resolveDirectory(Path testFile, String directoryName) {
        if (directoryName == null || directoryName.isEmpty()) {
            return null;
        }
        var resolved = testFile.getParent().resolve(directoryName);
        if (Files.isDirectory(resolved)) {
            return resolved.toAbsolutePath().toString();
        }
        var url = getClass().getClassLoader().getResource(directoryName);
        if (url != null && "file".equals(url.getProtocol())) {
            return url.getPath();
        }
        var absolute = Path.of(directoryName);
        if (Files.isDirectory(absolute)) {
            return absolute.toAbsolutePath().toString();
        }
        return directoryName;
    }

    private String readDefinitionContent(Path testFile, String definitionPath) throws java.io.IOException {
        try (var stream = getClass().getClassLoader().getResourceAsStream(definitionPath)) {
            if (stream != null) {
                return new String(stream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
            }
        }
        var resolved = testFile.getParent().resolve(definitionPath);
        if (Files.exists(resolved)) {
            return Files.readString(resolved);
        }
        var absolute = Path.of(definitionPath);
        if (Files.exists(absolute)) {
            return Files.readString(absolute);
        }
        throw new TestDefinitionException("Definition file not found: " + definitionPath);
    }

    /**
     * Expand the user-supplied paths into a flat, sorted list of YAML test files.
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
     * Walk the cause chain and join distinct messages with ": ", so the report
     * surfaces the actual reason a wrapped exception (e.g. DataException) failed.
     */
    static String describeFailure(Throwable t) {
        Objects.requireNonNull(t, "throwable cannot be null");

        var parts = new ArrayList<String>();
        var seen = new HashSet<String>();
        for (var current = t; current != null; current = current.getCause()) {
            var message = stripHelpfulNpeSuffix(current.getMessage());
            if (message != null && !message.isBlank() && seen.add(message)) parts.add(message);
            if (current.getCause() == current) break;
        }
        return parts.isEmpty() ? t.getClass().getSimpleName() : String.join(": ", parts);
    }

    private static String stripHelpfulNpeSuffix(String message) {
        if (message == null) return null;
        var marker = message.indexOf("Cannot invoke \"");
        if (marker < 0) return message;
        var end = marker;
        if (end >= 2 && message.charAt(end - 2) == ':' && message.charAt(end - 1) == ' ') end -= 2;
        return message.substring(0, end).stripTrailing();
    }

    /**
     * Sanitize a name into a valid Kafka Streams topology name (ASCII alnum, '.', '_', '-').
     */
    private static String sanitizeTopologyName(String name) {
        return name.replaceAll("[^A-Za-z0-9._-]", "_");
    }

    private static String filenameWithoutExtension(Path file) {
        var name = file.getFileName().toString();
        var dot = name.lastIndexOf('.');
        return dot > 0 ? name.substring(0, dot) : name;
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
                    System.out.println("  PASS  " + result.qualifiedLabel());
                    passed++;
                }
                case FAIL -> {
                    System.out.println("  FAIL  " + result.qualifiedLabel());
                    System.out.println("        " + result.message());
                    failed++;
                }
                case ERROR -> {
                    System.out.println("  ERROR " + result.qualifiedLabel());
                    System.out.println("        " + result.message());
                    errored++;
                }
            }
        }

        System.out.println();
        System.out.printf("%d passed, %d failed, %d errors%n", passed, failed, errored);
    }
}
