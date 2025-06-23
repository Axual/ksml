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

import static org.junit.platform.commons.support.AnnotationSupport.findAnnotation;
import static org.junit.platform.commons.support.AnnotationSupport.isAnnotated;


import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

/**
 * {@link TestTemplateInvocationContextProvider} that supports the {@link KSMLTopologyTest} annotation.
 */
@Slf4j
public class KSMLTopologyTestContextProvider implements TestTemplateInvocationContextProvider {

    @Override
    public boolean supportsTestTemplate(ExtensionContext context) {
        log.debug("Checking for KSMLTopologyTest annotation");
        return isAnnotated(context.getTestMethod(), KSMLTopologyTest.class);
    }

    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(ExtensionContext context) {
        log.debug("provideTestTemplateInvocationContexts()");

        Class<?> requiredTestClass = context.getRequiredTestClass();
        Map<String, KSMLTopic> inputTopics = new HashMap<>();
        Map<String, KSMLTopic> outputTopics = new HashMap<>();
        AtomicReference<String> testDriverRef = new AtomicReference<>();
        Field[] declaredFields = requiredTestClass.getDeclaredFields();

        log.debug("Scanning class {} for annotated fields", requiredTestClass.getName());
        Arrays.stream(declaredFields).forEach(field -> {
            Class<?> type = field.getType();
            if (type.equals(TestInputTopic.class) && field.isAnnotationPresent(KSMLTopic.class)) {
                KSMLTopic ksmlTopic = field.getAnnotation(KSMLTopic.class);
                log.debug("Found annotated input topic field {}:{}", field.getName(), ksmlTopic);
                inputTopics.put(field.getName(), ksmlTopic);
            } else if (type.equals(TestOutputTopic.class) && field.isAnnotationPresent(KSMLTopic.class)) {
                KSMLTopic ksmlTopic = field.getAnnotation(KSMLTopic.class);
                log.debug("Found annotated output topic field {}:{}", field.getName(), ksmlTopic);
                outputTopics.put(field.getName(), ksmlTopic);
            } else if (type.equals(TopologyTestDriver.class) && field.isAnnotationPresent(KSMLDriver.class)) {
                log.debug("Found annotated test driver field {}", field.getName());
                testDriverRef.set(field.getName());
            }
        });

        Method testMethod = context.getRequiredTestMethod();
        KSMLTopologyTest ksmlTopologyTest = findAnnotation(testMethod, KSMLTopologyTest.class).get();
        final String schemaDirectory = ksmlTopologyTest.schemaDirectory();

        // one-time preparation for the test runs: register notations
        log.debug("Registering test notations");
        final var mapper = new NativeDataObjectMapper();
        final var jsonNotation = new JsonNotation("json", mapper);
        ExecutionContext.INSTANCE.notationLibrary().register(new BinaryNotation(UserType.DEFAULT_NOTATION, mapper, jsonNotation::serde));
        ExecutionContext.INSTANCE.notationLibrary().register(jsonNotation);

        return Arrays.stream(ksmlTopologyTest.topologies())
                .map(topologyName -> new KSMLTopologyTestInvocationContext(topologyName, schemaDirectory, inputTopics, outputTopics, testDriverRef.get())
        );
    }
}
