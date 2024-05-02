package io.axual.ksml;

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
import io.axual.ksml.testutil.KSMLTestBase;
import io.axual.ksml.testutil.KSMLTestExtension;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;

@Slf4j
@ExtendWith(KSMLTestExtension.class)
public class KSMLFilterTest extends KSMLTestBase {

    @Test
    @KSMLTest(topology="pipelines/2-demo.yaml", inputTopic="ksml_sensordata_avro", outputTopic="ksml_sensordata_copy")
    void testCopying() throws Exception {
        log.debug("testCopying()");

        inputTopic.pipeInput("key1", "value1");
        var keyValue = outputTopic.readKeyValue();
        System.out.printf("Output topic key=%s, value=%s\n", keyValue.key, keyValue.value);
    }


    @Test
    void testFilterAvroRecords() throws Exception {
        log.debug("testFilterAvroRecords()");

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

            inputTopic = driver.createInputTopic("ksml_sensordata_avro", new StringSerializer(), kafkaAvroSerializer);
            outputTopic = driver.createOutputTopic("ksml_sensordata_filtered", new StringDeserializer(), kafkaAvroDeserializer);

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
}
