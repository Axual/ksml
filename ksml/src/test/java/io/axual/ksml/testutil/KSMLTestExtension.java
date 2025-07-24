package io.axual.ksml.testutil;

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
import com.google.common.collect.ImmutableMap;
import io.axual.ksml.TopologyGenerator;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.notation.avro.AvroNotation;
import io.axual.ksml.data.notation.avro.confluent.ConfluentAvroNotationProvider;
import io.axual.ksml.data.notation.avro.confluent.ConfluentAvroSerdeSupplier;
import io.axual.ksml.data.notation.binary.BinaryNotation;
import io.axual.ksml.data.notation.confluent.MockConfluentSchemaRegistryClient;
import io.axual.ksml.data.notation.json.JsonNotation;
import io.axual.ksml.definition.parser.TopologyDefinitionParser;
import io.axual.ksml.execution.ExecutionContext;
import io.axual.ksml.generator.YAMLObjectMapper;
import io.axual.ksml.parser.ParseNode;
import io.axual.ksml.type.UserType;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.graalvm.home.Version;
import org.junit.jupiter.api.extension.*;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

@Slf4j
public class KSMLTestExtension implements ExecutionCondition, BeforeAllCallback, BeforeEachCallback, AfterEachCallback {

    private final Set<Field> modifiedFields = new HashSet<>();

    private TopologyTestDriver topologyTestDriver;

    /**
     * Map of annotated {@link TestInputTopic} field names to their annotation.
     */
    private final Map<String, KSMLTopic> inputTopics = new HashMap<>();

    /**
     * Map of annotated {@link org.apache.kafka.streams.TestOutputTopic} to their annotations.
     */
    private final Map<String, KSMLTopic> outputTopics = new HashMap<>();

    /**
     * If present, name of a field of type {@link TopologyTestDriver} which should be linked by the extension.
     */
    private String testDriverRef;

    /**
     * Check if the test(s) are running on GraalVM.
     *
     * @param extensionContext the extension context
     * @return enabled only if the test is running on GraalVM
     */
    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(final ExtensionContext extensionContext) {
        if (extensionContext.getTestMethod().isEmpty()) {
            // at class level verification
            log.debug("Check for GraalVM");
            if (Version.getCurrent().isRelease()) {
                return ConditionEvaluationResult.enabled("running on GraalVM");
            }
            log.warn("KSML tests need GraalVM to run, test disabled");
            extensionContext.publishReportEntry("KSML tests need GraalVM, test disabled");
            return ConditionEvaluationResult.disabled("KSML tests need GraalVM to run");
        }
        return ConditionEvaluationResult.enabled("on method");
    }

    /**
     * Register the required notations before executing the tests.
     */
    @Override
    public void beforeAll(ExtensionContext extensionContext) {
        log.debug("Registering test notations");
        final var mapper = new NativeDataObjectMapper();
        final var jsonNotation = new JsonNotation(mapper);
        ExecutionContext.INSTANCE.notationLibrary().register(UserType.DEFAULT_NOTATION, new BinaryNotation(mapper, jsonNotation::serde));
        ExecutionContext.INSTANCE.notationLibrary().register(JsonNotation.NOTATION_NAME, jsonNotation);

        log.debug("Registering annotated TestInputTopic, TestOutputTopic and TopologyTestDriver fields");
        final var requiredTestClass = extensionContext.getRequiredTestClass();
        final var declaredFields = requiredTestClass.getDeclaredFields();
        Arrays.stream(declaredFields).forEach(field -> {
            final var type = field.getType();
            if (type.equals(TestInputTopic.class) && field.isAnnotationPresent(KSMLTopic.class)) {
                final var ksmlTopic = field.getAnnotation(KSMLTopic.class);
                log.debug("Found annotated input topic field {}:{}", field.getName(), ksmlTopic);
                inputTopics.put(field.getName(), ksmlTopic);
            } else if (type.equals(org.apache.kafka.streams.TestOutputTopic.class) && field.isAnnotationPresent(KSMLTopic.class)) {
                final var ksmlTopic = field.getAnnotation(KSMLTopic.class);
                log.debug("Found annotated output topic field {}:{}", field.getName(), ksmlTopic);
                outputTopics.put(field.getName(), ksmlTopic);
            } else if (type.equals(TopologyTestDriver.class) && field.isAnnotationPresent(KSMLDriver.class)) {
                log.debug("Found annotated test driver field {}", field.getName());
                testDriverRef = field.getName();
            }
        });
    }

    @Override
    public void beforeEach(final ExtensionContext extensionContext) throws Exception {
        final var streamsBuilder = new StreamsBuilder();
        if (extensionContext.getTestMethod().isEmpty()) {
            return;
        }

        // get the annotation on the method
        log.debug("Setting up KSML test");
        final var method = extensionContext.getTestMethod().get();
        final var methodName = method.getName();
        final var ksmlTest = method.getAnnotation(KSMLTest.class);
        if (ksmlTest == null) return;

        if (!KSMLTest.NO_SCHEMAS.equals(ksmlTest.schemaDirectory())) {
            log.debug("Annotated schema directory: `{}`", ksmlTest.schemaDirectory());
            final var schemaDirectoryURI = ClassLoader.getSystemResource(ksmlTest.schemaDirectory()).toURI();
            final var schemaDirectory = schemaDirectoryURI.getPath();
            ExecutionContext.INSTANCE.schemaLibrary().schemaDirectory(schemaDirectory);
            log.debug("Registered schema directory: {}", schemaDirectory);
        } else {
            ExecutionContext.INSTANCE.schemaLibrary().schemaDirectory(KSMLTest.NO_SCHEMAS);
        }

        final var registryClient = new MockConfluentSchemaRegistryClient();
        final var mockAvroNotation = new ConfluentAvroNotationProvider(registryClient).createNotation();
        ExecutionContext.INSTANCE.notationLibrary().register(AvroNotation.NOTATION_NAME, mockAvroNotation);

        // Get the KSML definition classpath relative path and load the topology into the test driver
        final var topologyName = ksmlTest.topology();
        log.debug("Loading topology {}", topologyName);
        final var uri = ClassLoader.getSystemResource(topologyName).toURI();
        final var path = Paths.get(uri);
        final var definition = YAMLObjectMapper.INSTANCE.readValue(Files.readString(path), JsonNode.class);
        final var definitions = ImmutableMap.of("definition",
                new TopologyDefinitionParser("test").parse(ParseNode.fromRoot(definition, methodName)));
        final var topologyGenerator = new TopologyGenerator(methodName + ".app");
        final var topology = topologyGenerator.create(streamsBuilder, definitions);
        final var description = topology.describe();
        log.info("{}", description);
        topologyTestDriver = new TopologyTestDriver(topology);

        // create in- and output topics and assign them to variables in the test
        final var testClass = extensionContext.getRequiredTestClass();
        final var testInstance = extensionContext.getRequiredTestInstance();

        log.debug("Registering annotated fields");
        for (final var entry : inputTopics.entrySet()) {
            final var fieldName = entry.getKey();
            final var ksmlTopic = entry.getValue();
            log.debug("Set variable {} to topic {}", fieldName, ksmlTopic.topic());
            final var inputTopicField = testClass.getDeclaredField(fieldName);
            inputTopicField.setAccessible(true);
            inputTopicField.set(testInstance, topologyTestDriver.createInputTopic(ksmlTopic.topic(), getKeySerializer(ksmlTopic), getValueSerializer(ksmlTopic)));
            modifiedFields.add(inputTopicField);
        }
        for (final var entry : outputTopics.entrySet()) {
            final var fieldName = entry.getKey();
            final var ksmlTopic = entry.getValue();
            log.debug("Set variable {} to topic {}", fieldName, ksmlTopic.topic());
            final var outputTopicField = testClass.getDeclaredField(fieldName);
            outputTopicField.setAccessible(true);
            outputTopicField.set(testInstance, topologyTestDriver.createOutputTopic(ksmlTopic.topic(), getKeyDeserializer(ksmlTopic), getValueDeserializer(ksmlTopic)));
            modifiedFields.add(outputTopicField);
        }

        // if a variable is configured for the test driver reference, set the reference
        if (testDriverRef != null) {
            log.debug("Set variable {} to test driver", testDriverRef);
            final var testDriverField = testClass.getDeclaredField(testDriverRef);
            testDriverField.setAccessible(true);
            testDriverField.set(testInstance, topologyTestDriver);
            modifiedFields.add(testDriverField);
        }
    }

    @Override
    public void afterEach(final ExtensionContext context) throws Exception {
        if (context.getTestMethod().isEmpty()) {
            // not at method level
            return;
        }
        final var method = context.getTestMethod().get();
        final var annotation = method.getAnnotation(KSMLTest.class);
        if (annotation == null) {
            // method was not annotated, nothing to do
            return;
        }

        // clean up
        log.debug("Clean up topology test driver");
        if (topologyTestDriver != null) {
            topologyTestDriver.close();
            topologyTestDriver = null;
        }

        // clear any set fields
        log.debug("clean up test instance variables");
        final var testInstance = context.getRequiredTestInstance();
        for (final var field : modifiedFields) {
            log.debug("Clearing {}", field.getName());
            field.setAccessible(true);
            field.set(testInstance, null);
        }
        modifiedFields.clear();
    }

    private Serializer<?> getKeySerializer(final KSMLTopic ksmlTopic) {
        return getSerializer(ksmlTopic, true);
    }

    private Serializer<?> getValueSerializer(final KSMLTopic ksmlTopic) {
        return getSerializer(ksmlTopic, false);
    }

    private Serializer<?> getSerializer(final KSMLTopic ksmlTopic, final boolean isKey) {
        return switch (isKey ? ksmlTopic.keySerde() : ksmlTopic.valueSerde()) {
            case AVRO -> {
                final var avroNotation = (AvroNotation) ExecutionContext.INSTANCE.notationLibrary().get(AvroNotation.NOTATION_NAME);
                final var mockRegistry = (MockConfluentSchemaRegistryClient) ((ConfluentAvroSerdeSupplier) avroNotation.serdeSupplier()).registryClient();
                final var result = new KafkaAvroSerializer(mockRegistry);
                result.configure(mockRegistry.configs(), isKey);
                yield result;
            }
            case STRING -> new StringSerializer();
            case LONG -> new LongSerializer();
            case INTEGER -> new IntegerSerializer();
        };
    }

    private Deserializer<?> getKeyDeserializer(final KSMLTopic ksmlTopic) {
        return getDeserializer(ksmlTopic, true);
    }

    private Deserializer<?> getValueDeserializer(final KSMLTopic ksmlTopic) {
        return getDeserializer(ksmlTopic, false);
    }

    private Deserializer<?> getDeserializer(final KSMLTopic ksmlTopic, final boolean isKey) {
        return switch (isKey ? ksmlTopic.keySerde() : ksmlTopic.valueSerde()) {
            case AVRO -> {
                final var avroNotation = (AvroNotation) ExecutionContext.INSTANCE.notationLibrary().get(AvroNotation.NOTATION_NAME);
                final var mockRegistry = (MockConfluentSchemaRegistryClient) ((ConfluentAvroSerdeSupplier) avroNotation.serdeSupplier()).registryClient();
                final var result = new KafkaAvroDeserializer(mockRegistry);
                result.configure(mockRegistry.configs(), isKey);
                yield result;
            }
            case STRING -> new StringDeserializer();
            case LONG -> new LongDeserializer();
            case INTEGER -> new IntegerDeserializer();
        };
    }
}
