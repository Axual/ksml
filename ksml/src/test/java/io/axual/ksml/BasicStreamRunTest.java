package io.axual.ksml;

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
import io.axual.ksml.data.mapper.DataObjectFlattener;
import io.axual.ksml.data.notation.avro.AvroNotation;
import io.axual.ksml.data.notation.avro.confluent.ConfluentAvroSerdeProvider;
import io.axual.ksml.data.notation.binary.BinaryNotation;
import io.axual.ksml.data.notation.confluent.MockConfluentSchemaRegistryClient;
import io.axual.ksml.data.notation.json.JsonNotation;
import io.axual.ksml.definition.parser.TopologyDefinitionParser;
import io.axual.ksml.execution.ExecutionContext;
import io.axual.ksml.generator.YAMLObjectMapper;
import io.axual.ksml.operation.SensorData;
import io.axual.ksml.parser.ParseNode;
import io.axual.ksml.type.UserType;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.TopologyTestDriver;
import org.graalvm.home.Version;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Basic stream run test.
 * See {@link io.axual.ksml.testutil.KSMLTestExtension} for a solution that abstracts away the boilerplate code.
 */
@EnabledIf(value = "onGraalVM", disabledReason = "This test needs GraalVM to run")
class BasicStreamRunTest {
    private final StreamsBuilder streamsBuilder = new StreamsBuilder();

    @BeforeAll
    static void setup() {
        final var mapper = new DataObjectFlattener();
        final var jsonNotation = new JsonNotation(mapper);
        ExecutionContext.INSTANCE.notationLibrary().register(UserType.DEFAULT_NOTATION, new BinaryNotation(mapper, jsonNotation::serde));
        ExecutionContext.INSTANCE.notationLibrary().register(JsonNotation.NOTATION_NAME, jsonNotation);

        try {
            final var schemaDirectoryURI = ClassLoader.getSystemResource("schemas").toURI();
            final var schemaDirectory = schemaDirectoryURI.getPath();
            System.out.println("schemaDirectory = " + schemaDirectory);
            ExecutionContext.INSTANCE.schemaLibrary().schemaDirectory(schemaDirectory);
        } catch (URISyntaxException e) {
            // Ignore
        }
    }

    @Test
    void parseAndCheckOutput() throws Exception {
        final var uri = ClassLoader.getSystemResource("pipelines/test-copying.yaml").toURI();
        final var path = Paths.get(uri);
        final var definition = YAMLObjectMapper.INSTANCE.readValue(Files.readString(path), JsonNode.class);
        final var definitions = ImmutableMap.of("definition",
                new TopologyDefinitionParser("test").parse(ParseNode.fromRoot(definition, "test")));
        var topologyGenerator = new TopologyGenerator("some.app.id");
        final var topology = topologyGenerator.create(streamsBuilder, definitions);
        final TopologyDescription description = topology.describe();
        System.out.println(description);

        try (TopologyTestDriver driver = new TopologyTestDriver(topology)) {
            TestInputTopic<String, String> inputTopic = driver.createInputTopic("ksml_sensordata_avro", new StringSerializer(), new StringSerializer());
            var outputTopic = driver.createOutputTopic("ksml_sensordata_copy", new StringDeserializer(), new StringDeserializer());
            inputTopic.pipeInput("key1", "value1");
            var keyValue = outputTopic.readKeyValue();
            assertEquals("key1", keyValue.key);
            assertEquals("value1", keyValue.value);
        }
    }

    @Test
    void testFilterAvroRecords() throws Exception {
        final var registryClient = new MockConfluentSchemaRegistryClient();
        final var serdeProvider = new ConfluentAvroSerdeProvider(registryClient);
        final var avroNotation = new AvroNotation(serdeProvider, registryClient.configs());
        ExecutionContext.INSTANCE.notationLibrary().register(AvroNotation.NOTATION_NAME, avroNotation);

        final var uri = ClassLoader.getSystemResource("pipelines/test-filter.yaml").toURI();
        final var path = Paths.get(uri);
        final var definition = YAMLObjectMapper.INSTANCE.readValue(Files.readString(path), JsonNode.class);
        final var definitions = ImmutableMap.of("definition",
                new TopologyDefinitionParser("test").parse(ParseNode.fromRoot(definition, "test")));
        var topologyGenerator = new TopologyGenerator("some.app.id");
        final var topology = topologyGenerator.create(streamsBuilder, definitions);
        final TopologyDescription description = topology.describe();
        System.out.println(description);

        try (TopologyTestDriver driver = new TopologyTestDriver(topology)) {
            final var kafkaAvroSerializer = new KafkaAvroSerializer(registryClient);
            kafkaAvroSerializer.configure(registryClient.configs(), false);

            final var kafkaAvroDeserializer = new KafkaAvroDeserializer(registryClient);
            kafkaAvroDeserializer.configure(registryClient.configs(), false);

            TestInputTopic<String, Object> avroInputTopic = driver.createInputTopic("ksml_sensordata_avro", new StringSerializer(), kafkaAvroSerializer);
            var outputTopic = driver.createOutputTopic("ksml_sensordata_filtered", new StringDeserializer(), kafkaAvroDeserializer);

            SensorData build = SensorData.builder().name("name").timestamp(System.currentTimeMillis()).value("AMS").type(SensorData.SensorType.AREA).unit("u").color("blue").build();

            avroInputTopic.pipeInput("key1", build.toRecord());
            if (!outputTopic.isEmpty()) {
                var keyValue = outputTopic.readKeyValue();
                assertEquals("key1", keyValue.key);
                System.out.printf("Output topic key=%s, value=%s%n", keyValue.key, keyValue.value);
            }
        }
    }

    static boolean onGraalVM() {
        return Version.getCurrent().isRelease();
    }
}
