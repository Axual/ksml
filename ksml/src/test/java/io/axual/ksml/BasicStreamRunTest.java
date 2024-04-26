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

import com.google.common.collect.ImmutableMap;

import com.fasterxml.jackson.databind.JsonNode;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.TopologyTestDriver;
import org.graalvm.home.Version;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;

import io.axual.ksml.data.notation.NotationLibrary;
import io.axual.ksml.data.notation.avro.AvroSchemaLoader;
import io.axual.ksml.data.notation.avro.MockAvroNotation;
import io.axual.ksml.data.notation.binary.BinaryNotation;
import io.axual.ksml.data.notation.json.JsonDataObjectConverter;
import io.axual.ksml.data.notation.json.JsonNotation;
import io.axual.ksml.data.parser.ParseNode;
import io.axual.ksml.data.schema.SchemaLibrary;
import io.axual.ksml.definition.parser.TopologyDefinitionParser;
import io.axual.ksml.generator.YAMLObjectMapper;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.Builder;

@EnabledIf(value = "onGraalVM", disabledReason = "This test needs GraalVM to run")
public class BasicStreamRunTest {

    private final StreamsBuilder streamsBuilder = new StreamsBuilder();

    private TestInputTopic<String, String> inputTopic;

    private TestInputTopic<String, Object> avroInputTopic;

    @Test
    void parseAndCheckOuput() throws Exception {
        final var jsonNotation = new JsonNotation();
        NotationLibrary.register(BinaryNotation.NOTATION_NAME, new BinaryNotation(jsonNotation::serde), null);
        NotationLibrary.register(JsonNotation.NOTATION_NAME, jsonNotation, new JsonDataObjectConverter());

        final var uri = ClassLoader.getSystemResource("pipelines/2-demo.yaml").toURI();
        final var path = Paths.get(uri);
        final var definition = YAMLObjectMapper.INSTANCE.readValue(Files.readString(path), JsonNode.class);
        final var definitions = ImmutableMap.of("definition",
                new TopologyDefinitionParser("test").parse(ParseNode.fromRoot(definition, "test")));
        var topologyGenerator = new TopologyGenerator("some.app.id");
        final var topology = topologyGenerator.create(streamsBuilder, definitions);
        final TopologyDescription description = topology.describe();
        System.out.println(description);

        TopologyTestDriver driver = new TopologyTestDriver(topology);
        inputTopic = driver.createInputTopic("ksml_sensordata_avro", new StringSerializer(), new StringSerializer());
        var outputTopic = driver.createOutputTopic("ksml_sensordata_copy", new StringDeserializer(), new StringDeserializer());
        inputTopic.pipeInput("key1", "value1");
        var keyValue = outputTopic.readKeyValue();
        System.out.printf("Output topic key=%s, value=%s\n", keyValue.key, keyValue.value);

    }

    @Test
    void testFilterAvroRecords() throws Exception {
        final var jsonNotation = new JsonNotation();
        NotationLibrary.register(BinaryNotation.NOTATION_NAME, new BinaryNotation(jsonNotation::serde), null);
        NotationLibrary.register(JsonNotation.NOTATION_NAME, jsonNotation, new JsonDataObjectConverter());

        final var avroNotation = new MockAvroNotation(new HashMap<>());
        NotationLibrary.register(MockAvroNotation.NOTATION_NAME, avroNotation, null);

        URI testDirectory = ClassLoader.getSystemResource("pipelines").toURI();
        String schemaPath = testDirectory.getPath();
        System.out.println("schemaPath = " + schemaPath);
        SchemaLibrary.registerLoader(MockAvroNotation.NOTATION_NAME, new AvroSchemaLoader(schemaPath));

        final var uri = ClassLoader.getSystemResource("pipelines/03-example-filter.yaml").toURI();
        final var path = Paths.get(uri);
        final var definition = YAMLObjectMapper.INSTANCE.readValue(Files.readString(path), JsonNode.class);
        final var definitions = ImmutableMap.of("definition",
                new TopologyDefinitionParser("test").parse(ParseNode.fromRoot(definition, "test")));
        var topologyGenerator = new TopologyGenerator("some.app.id");
        final var topology = topologyGenerator.create(streamsBuilder, definitions);
        final TopologyDescription description = topology.describe();
        System.out.println(description);

        final var sensorDataSchema = new Schema.Parser().parse(ClassLoader.getSystemResourceAsStream("pipelines/SensorData.avsc"));
        final var sensorTypeSchema = sensorDataSchema.getField("type").schema();

        try (TopologyTestDriver driver = new TopologyTestDriver(topology)) {
            final var kafkaAvroSerializer = new KafkaAvroSerializer(avroNotation.mockSchemaRegistryClient());
            kafkaAvroSerializer.configure(avroNotation.getSchemaRegistryConfigs(), false);

            final var kafkaAvroDeserializer = new KafkaAvroDeserializer(avroNotation.mockSchemaRegistryClient());
            kafkaAvroDeserializer.configure(avroNotation.getSchemaRegistryConfigs(), false);

            avroInputTopic = driver.createInputTopic("ksml_sensordata_avro", new StringSerializer(), kafkaAvroSerializer);
            var outputTopic = driver.createOutputTopic("ksml_sensordata_filtered", new StringDeserializer(), kafkaAvroDeserializer);

            GenericRecord data = new GenericData.Record(sensorDataSchema);
            data.put("name", "test-name");
            data.put("timestamp", System.currentTimeMillis());
            data.put("value", "AMS");
            data.put("type", new GenericData.EnumSymbol(sensorTypeSchema, "AREA"));
            data.put("unit", "u");
            data.put("color", "blue");

            avroInputTopic.pipeInput("key1", data);
            var keyValue = outputTopic.readKeyValue();
            System.out.printf("Output topic key=%s, value=%s\n", keyValue.key, keyValue.value);
        }

    }

    static boolean onGraalVM() {
        return Version.getCurrent().isRelease();
    }

    @Builder
    static class SensorData {
        String name;
        long timestamp;
        String value;
        SensorType type;
        String unit;
    }

    static enum SensorType {
        AREA, HUMIDITY,LENGTH,STATE,TEMPERATURE
    }

}
