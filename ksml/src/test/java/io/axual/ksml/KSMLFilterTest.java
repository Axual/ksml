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
import io.axual.ksml.data.notation.NotationLibrary;
import io.axual.ksml.data.notation.avro.AvroSchemaLoader;
import io.axual.ksml.data.notation.avro.MockAvroNotation;
import io.axual.ksml.data.parser.ParseNode;
import io.axual.ksml.data.schema.SchemaLibrary;
import io.axual.ksml.definition.parser.TopologyDefinitionParser;
import io.axual.ksml.generator.YAMLObjectMapper;
import io.axual.ksml.testutil.KSMLTest;
import io.axual.ksml.testutil.KSMLTestExtension;
import io.axual.ksml.testutil.KSMLTopic;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * @deprecated This class is superseded by {@link KSMLCopyAndFilterTest} and should probably be deleted.
 */
@Slf4j
@Deprecated(forRemoval = true)
@ExtendWith({KSMLTestExtension.class})
class KSMLFilterTest {

    protected final StreamsBuilder streamsBuilder = new StreamsBuilder();

    protected TestInputTopic inputTopic;

    protected TestOutputTopic outputTopic;

    TestInputTopic<String, String> myInput;

    TestOutputTopic<String, String> myOutput;

    @KSMLTest(topology= "pipelines/test-copying.yaml",
            inputTopics = {@KSMLTopic(variable="myInput", topic="ksml_sensordata_avro")},
            outputTopics = {@KSMLTopic(variable="myOutput", topic="ksml_sensordata_copy")})
    void testCopying() throws Exception {
        log.debug("testCopying()");

        myInput.pipeInput("key1", "value1");
        assertFalse(myOutput.isEmpty(), "record should be copied");
        var keyValue = myOutput.readKeyValue();
        System.out.printf("Output topic key=%s, value=%s\n", keyValue.key, keyValue.value);
    }

    @Test
    void testFilterAvroRecordsOld() throws Exception {
        log.debug("testFilterAvroRecords()");

        final var avroNotation = new MockAvroNotation(new HashMap<>());
        NotationLibrary.register(MockAvroNotation.NOTATION_NAME, avroNotation, null);

        URI testDirectory = ClassLoader.getSystemResource("pipelines").toURI();
        String schemaPath = testDirectory.getPath();
        System.out.println("schemaPath = " + schemaPath);
        SchemaLibrary.registerLoader(MockAvroNotation.NOTATION_NAME, new AvroSchemaLoader(schemaPath));

        final var uri = ClassLoader.getSystemResource("pipelines/test-filtering.yaml").toURI();
        final var path = Paths.get(uri);
        final var definition = YAMLObjectMapper.INSTANCE.readValue(Files.readString(path), JsonNode.class);
        final var definitions = ImmutableMap.of("definition",
                new TopologyDefinitionParser("test").parse(ParseNode.fromRoot(definition, "test")));
        var topologyGenerator = new TopologyGenerator("some.app.id");
        final var topology = topologyGenerator.create(streamsBuilder, definitions);
        final TopologyDescription description = topology.describe();
        System.out.println(description);


        try (TopologyTestDriver driver = new TopologyTestDriver(topology)) {
            final var kafkaAvroSerializer = new KafkaAvroSerializer(avroNotation.mockSchemaRegistryClient());
            kafkaAvroSerializer.configure(avroNotation.getSchemaRegistryConfigs(), false);

            final var kafkaAvroDeserializer = new KafkaAvroDeserializer(avroNotation.mockSchemaRegistryClient());
            kafkaAvroDeserializer.configure(avroNotation.getSchemaRegistryConfigs(), false);

            inputTopic = driver.createInputTopic("ksml_sensordata_avro", new StringSerializer(), kafkaAvroSerializer);
            outputTopic = driver.createOutputTopic("ksml_sensordata_filtered", new StringDeserializer(), kafkaAvroDeserializer);

            // schema's only used for sending data
            final var sensorDataSchema = new Schema.Parser().parse(ClassLoader.getSystemResourceAsStream("pipelines/SensorData.avsc"));
            final var sensorTypeSchema = sensorDataSchema.getField("type").schema();

            GenericRecord data = new GenericData.Record(sensorDataSchema);
            data.put("name", "test-name");
            data.put("timestamp", System.currentTimeMillis());
            data.put("value", "AMS");
            data.put("type", new GenericData.EnumSymbol(sensorTypeSchema, "AREA"));
            data.put("unit", "u");
            data.put("color", "blue");

            inputTopic.pipeInput("key1", data);
            if (!outputTopic.isEmpty()) {
                var keyValue = outputTopic.readKeyValue();
                System.out.printf("Output topic key=%s, value=%s\n", keyValue.key, keyValue.value);
            }
        }
    }

    @KSMLTest(topology = "pipelines/test-filtering.yaml", schemapath = "pipelines",
            inputTopics = {@KSMLTopic(variable = "inputTopic", topic = "ksml_sensordata_avro", valueSerde = KSMLTopic.SerdeType.AVRO)},
            outputTopics = {@KSMLTopic(variable = "outputTopic", topic = "ksml_sensordata_filtered", valueSerde = KSMLTopic.SerdeType.AVRO)})
    void testFilterAvroRecords() throws Exception {
        log.debug("testFilterAvroRecords()");
        final var sensorDataSchema = new Schema.Parser().parse(ClassLoader.getSystemResourceAsStream("pipelines/SensorData.avsc"));
        final var sensorTypeSchema = sensorDataSchema.getField("type").schema();

        GenericRecord data = new GenericData.Record(sensorDataSchema);
        data.put("name", "test-name");
        data.put("timestamp", System.currentTimeMillis());
        data.put("value", "AMS");
        data.put("type", new GenericData.EnumSymbol(sensorTypeSchema, "AREA"));
        data.put("unit", "u");
        data.put("color", "blue");

        inputTopic.pipeInput("key1", data);
        if (!outputTopic.isEmpty()) {
            var keyValue = outputTopic.readKeyValue();
            System.out.printf("Output topic key=%s, value=%s\n", keyValue.key, keyValue.value);
        }
    }
}
