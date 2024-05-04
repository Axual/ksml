package io.axual.ksml.testutil;

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
import org.apache.kafka.streams.*;
import org.graalvm.home.Version;
import org.junit.jupiter.api.extension.*;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Paths;

@Slf4j
public class KSMLTestExtension implements ExecutionCondition, BeforeAllCallback, BeforeEachCallback {

    private final StreamsBuilder streamsBuilder = new StreamsBuilder();

    /**
     * Check if the test(s) are running on GraalVM.
     * @param extensionContext the extension context
     * @return enabled only if the test is running on GraalVM
     */
    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext extensionContext) {
        if (extensionContext.getTestMethod().isEmpty()) {
            // class level verification
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
            // no annotation
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
        TopologyTestDriver testDriver = new TopologyTestDriver(topology);

        // set up topology and assign in- and output topics
        Class<?> testClass = extensionContext.getRequiredTestClass();
        Field inputTopicField = testClass.getSuperclass().getDeclaredField("inputTopic");
        Field outputTopicField = testClass.getSuperclass().getDeclaredField("outputTopic");

        Object testInstance = extensionContext.getRequiredTestInstance();
        inputTopicField.set(testInstance, testDriver.createInputTopic(ksmlTest.inputTopic(), new StringSerializer(), new StringSerializer()));
        outputTopicField.set(testInstance, testDriver.createOutputTopic(ksmlTest.outputTopic(), new StringDeserializer(), new StringDeserializer()));

    }

}
