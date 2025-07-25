package io.axual.ksml.testutil;

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

import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.notation.NotationContext;
import io.axual.ksml.data.notation.binary.BinaryNotation;
import io.axual.ksml.data.notation.json.JsonNotation;
import io.axual.ksml.execution.ExecutionContext;
import io.axual.ksml.type.UserType;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.junit.platform.commons.support.AnnotationSupport.isAnnotated;

/**
 * {@link TestTemplateInvocationContextProvider} that supports the {@link KSMLTopologyTest} annotation.
 */
@Slf4j
public class KSMLTopologyTestContextProvider implements TestTemplateInvocationContextProvider {

    @Override
    public boolean supportsTestTemplate(final ExtensionContext context) {
        log.debug("Checking for KSMLTopologyTest annotation");
        return isAnnotated(context.getTestMethod(), KSMLTopologyTest.class);
    }

    /**
     * Set up test template invocation contexts for each listed pipeline definition.
     * This method prepares test execution by scanning the test class for annotated fields ({@link KSMLTopic} and {@link KSMLDriver}
     * annotated fields) which it will pass into each invocation context, to be handled by {@link KSMLTopologyTestExtension} when the test runs.
     *
     * @param context the extension context.
     * @return a list containing one {@link KSMLTopologyTestInvocationContext} per configured KSML topology.
     */
    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(final ExtensionContext context) {
        log.debug("provideTestTemplateInvocationContexts()");

        final Map<String, KSMLTopic> inputTopics = new HashMap<>();
        final Map<String, KSMLTopic> outputTopics = new HashMap<>();
        AtomicReference<String> testDriverRef = new AtomicReference<>();

        final var requiredTestClass = context.getRequiredTestClass();
        final var declaredFields = requiredTestClass.getDeclaredFields();
        log.debug("Scanning class {} for annotated fields", requiredTestClass.getName());

        Arrays.stream(declaredFields).forEach(field -> {
            final var type = field.getType();
            if (type.equals(TestInputTopic.class) && field.isAnnotationPresent(KSMLTopic.class)) {
                final var ksmlTopic = field.getAnnotation(KSMLTopic.class);
                log.debug("Found annotated input topic field {}:{}", field.getName(), ksmlTopic);
                inputTopics.put(field.getName(), ksmlTopic);
            } else if (type.equals(TestOutputTopic.class) && field.isAnnotationPresent(KSMLTopic.class)) {
                final var ksmlTopic = field.getAnnotation(KSMLTopic.class);
                log.debug("Found annotated output topic field {}:{}", field.getName(), ksmlTopic);
                outputTopics.put(field.getName(), ksmlTopic);
            } else if (type.equals(TopologyTestDriver.class) && field.isAnnotationPresent(KSMLDriver.class)) {
                log.debug("Found annotated test driver field {}", field.getName());
                testDriverRef.set(field.getName());
            }
        });

        final var testMethod = context.getRequiredTestMethod();
        final var ksmlTopologyTest = testMethod.getAnnotation(KSMLTopologyTest.class);
        final var schemaDirectory = ksmlTopologyTest.schemaDirectory();

        // one-time preparation for the test runs: register notations
        log.debug("Registering notations in notationLibrary");
        final var mapper = new NativeDataObjectMapper();
        final var jsonNotation = new JsonNotation(new NotationContext(JsonNotation.NOTATION_NAME, mapper));
        ExecutionContext.INSTANCE.notationLibrary().register(UserType.DEFAULT_NOTATION, new BinaryNotation(new NotationContext(BinaryNotation.NOTATION_NAME, mapper), jsonNotation::serde));
        ExecutionContext.INSTANCE.notationLibrary().register(JsonNotation.NOTATION_NAME, jsonNotation);

        return Arrays.stream(ksmlTopologyTest.topologies())
                .map(topologyName -> new KSMLTopologyTestInvocationContext(topologyName, schemaDirectory, inputTopics, outputTopics, testDriverRef.get())
                );
    }
}
