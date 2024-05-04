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
import io.axual.ksml.data.notation.NotationLibrary;
import io.axual.ksml.data.notation.binary.BinaryNotation;
import io.axual.ksml.data.notation.json.JsonDataObjectConverter;
import io.axual.ksml.data.notation.json.JsonNotation;
import io.axual.ksml.data.parser.ParseNode;
import io.axual.ksml.definition.parser.TopologyDefinitionParser;
import io.axual.ksml.generator.YAMLObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.TopologyTestDriver;
import org.graalvm.home.Version;
import org.junit.jupiter.api.extension.*;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

@Slf4j
public class KSMLTestExtension implements ExecutionCondition, BeforeAllCallback, BeforeEachCallback, AfterEachCallback {

    private final StreamsBuilder streamsBuilder = new StreamsBuilder();

    private Set<Field> modifiedFields = new HashSet<>();

    private TopologyTestDriver topologyTestDriver;

    /**
     * Check if the test(s) are running on GraalVM.
     * @param extensionContext the extension context
     * @return enabled only if the test is running on GraalVM
     */
    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext extensionContext) {
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
     * @param extensionContext
     * @throws Exception
     */
    @Override
    public void beforeAll(ExtensionContext extensionContext) throws Exception {
        log.debug("registering notations");
        final var jsonNotation = new JsonNotation();
        NotationLibrary.register(BinaryNotation.NOTATION_NAME, new BinaryNotation(jsonNotation::serde), null);
        NotationLibrary.register(JsonNotation.NOTATION_NAME, jsonNotation, new JsonDataObjectConverter());

    }

    @Override
    public void beforeEach(ExtensionContext extensionContext) throws Exception {
        if (extensionContext.getTestMethod().isEmpty()) {
            return;
        }

        // get the annotation on the method
        Method method = extensionContext.getTestMethod().get();
        String methodName = method.getName();
        KSMLTest ksmlTest = method.getAnnotation(KSMLTest.class);

        if (ksmlTest == null) {
            // no annotation on method
            return;
        }

        // get the KSML definition classpath relative path and load it
        String topologyName = ksmlTest.topology();
        final var uri = ClassLoader.getSystemResource(topologyName).toURI();
        final var path = Paths.get(uri);
        final var definition = YAMLObjectMapper.INSTANCE.readValue(Files.readString(path), JsonNode.class);
        final var definitions = ImmutableMap.of("definition",
                new TopologyDefinitionParser("test").parse(ParseNode.fromRoot(definition, methodName)));
        var topologyGenerator = new TopologyGenerator(methodName + ".app");
        final var topology = topologyGenerator.create(streamsBuilder, definitions);
        final TopologyDescription description = topology.describe();
        System.out.println(description);
        topologyTestDriver = new TopologyTestDriver(topology);

        // set up topology and assign in- and output topics
        Class<?> testClass = extensionContext.getRequiredTestClass();
        Object testInstance = extensionContext.getRequiredTestInstance();
        for (KSMLTopic ksmlTopic: ksmlTest.inputTopics()) {
            log.debug("set variable {} to topic {}", ksmlTopic.variable(), ksmlTopic.topic());
            Field inputTopicField = testClass.getDeclaredField(ksmlTopic.variable());
            inputTopicField.setAccessible(true);
            inputTopicField.set(testInstance, topologyTestDriver.createInputTopic(ksmlTopic.topic(), new StringSerializer(), new StringSerializer()));
            modifiedFields.add(inputTopicField);
        }
        for (KSMLTopic ksmlTopic: ksmlTest.outputTopics()) {
            log.debug("set variable {} to topic {}", ksmlTopic.variable(), ksmlTopic.topic());
            Field outputTopicField = testClass.getDeclaredField(ksmlTopic.variable());
            outputTopicField.setAccessible(true);
            outputTopicField.set(testInstance, topologyTestDriver.createOutputTopic(ksmlTopic.topic(), new StringDeserializer(), new StringDeserializer()));
            modifiedFields.add(outputTopicField);
        }
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        if (context.getTestMethod().isEmpty()) {
            // not at method level
            return;
        }
        Method method = context.getTestMethod().get();
        var annotation = method.getAnnotation(KSMLTest.class);
        if (annotation == null) {
            // method was not annotated, nothing to do
            return;
        }

        // clean up
        topologyTestDriver.close();
        topologyTestDriver = null;

        // clear any set fields
        var testInstance = context.getRequiredTestInstance();
        for (Field field: modifiedFields) {
            field.setAccessible(true);
            field.set(testInstance, null);

        }
    }
}
