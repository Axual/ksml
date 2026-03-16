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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link TestDataProducer} using string-typed topics.
 */
class TestDataProducerTest {

    private static final String INPUT_TOPIC = "input";
    private static final String OUTPUT_TOPIC = "output";

    private TopologyTestDriver driver;

    @BeforeEach
    void setUp() {
        var stringSerde = Serdes.String();
        var builder = new StreamsBuilder();
        builder.stream(INPUT_TOPIC, Consumed.with(stringSerde, stringSerde))
                .to(OUTPUT_TOPIC, Produced.with(stringSerde, stringSerde));
        driver = new TopologyTestDriver(builder.build());
    }

    @AfterEach
    void tearDown() {
        if (driver != null) {
            driver.close();
        }
    }

    private org.apache.kafka.streams.TestOutputTopic<String, String> outputTopic() {
        return driver.createOutputTopic(OUTPUT_TOPIC, new StringDeserializer(), new StringDeserializer());
    }

    @Test
    void producesSingleMessage() {
        var producer = new TestDataProducer(driver);
        var block = new ProduceBlock(INPUT_TOPIC, "string", "string",
                List.of(new TestMessage("k1", "v1", null)),
                null, null);

        producer.produce(List.of(block));

        var records = outputTopic().readRecordsToList();
        assertEquals(1, records.size());
        assertEquals("k1", records.getFirst().key());
        assertEquals("v1", records.getFirst().value());
    }

    @Test
    void producesMultipleMessages() {
        var producer = new TestDataProducer(driver);
        var block = new ProduceBlock(INPUT_TOPIC, "string", "string",
                List.of(
                        new TestMessage("k1", "v1", null),
                        new TestMessage("k2", "v2", null),
                        new TestMessage("k3", "v3", null)),
                null, null);

        producer.produce(List.of(block));

        var records = outputTopic().readRecordsToList();
        assertEquals(3, records.size());
        assertEquals("k1", records.get(0).key());
        assertEquals("k2", records.get(1).key());
        assertEquals("k3", records.get(2).key());
    }

    @Test
    void producesWithExplicitTimestamp() {
        var producer = new TestDataProducer(driver);
        var block = new ProduceBlock(INPUT_TOPIC, "string", "string",
                List.of(new TestMessage("k1", "v1", 1709200000000L)),
                null, null);

        producer.produce(List.of(block));

        var records = outputTopic().readRecordsToList();
        assertEquals(1, records.size());
        assertEquals(1709200000000L, records.getFirst().timestamp());
    }

    @Test
    void producesWithMixedTimestamps() {
        var producer = new TestDataProducer(driver);
        var block = new ProduceBlock(INPUT_TOPIC, "string", "string",
                List.of(
                        new TestMessage("k1", "v1", 1000L),
                        new TestMessage("k2", "v2", null),
                        new TestMessage("k3", "v3", 3000L)),
                null, null);

        producer.produce(List.of(block));

        var records = outputTopic().readRecordsToList();
        assertEquals(3, records.size());
        assertEquals(1000L, records.get(0).timestamp());
        assertEquals(3000L, records.get(2).timestamp());
    }

    @Test
    void producesMultipleBlocks() {
        // Use the same topic for both blocks since our topology only has one input
        var producer = new TestDataProducer(driver);
        var block1 = new ProduceBlock(INPUT_TOPIC, "string", "string",
                List.of(new TestMessage("k1", "v1", null)),
                null, null);
        var block2 = new ProduceBlock(INPUT_TOPIC, "string", "string",
                List.of(new TestMessage("k2", "v2", null)),
                null, null);

        producer.produce(List.of(block1, block2));

        var records = outputTopic().readRecordsToList();
        assertEquals(2, records.size());
        assertEquals("k1", records.get(0).key());
        assertEquals("k2", records.get(1).key());
    }

    @Test
    void nullKeyTypeDefaultsToString() {
        var producer = new TestDataProducer(driver);
        var block = new ProduceBlock(INPUT_TOPIC, null, "string",
                List.of(new TestMessage("k1", "v1", null)),
                null, null);

        producer.produce(List.of(block));

        var records = outputTopic().readRecordsToList();
        assertEquals(1, records.size());
        assertEquals("k1", records.getFirst().key());
    }

    @Test
    void nullValueTypeDefaultsToString() {
        var producer = new TestDataProducer(driver);
        var block = new ProduceBlock(INPUT_TOPIC, "string", null,
                List.of(new TestMessage("k1", "v1", null)),
                null, null);

        producer.produce(List.of(block));

        var records = outputTopic().readRecordsToList();
        assertEquals(1, records.size());
        assertEquals("v1", records.getFirst().value());
    }

    @Test
    void blockWithNullMessagesIsSkipped() {
        var producer = new TestDataProducer(driver);
        var block = new ProduceBlock(INPUT_TOPIC, "string", "string",
                null, null, null);

        producer.produce(List.of(block));

        var records = outputTopic().readRecordsToList();
        assertEquals(0, records.size());
    }

    @Test
    void emptyBlockListProducesNothing() {
        var producer = new TestDataProducer(driver);

        producer.produce(List.of());

        var records = outputTopic().readRecordsToList();
        assertEquals(0, records.size());
    }

    @Test
    void invalidTypeThrowsException() {
        var producer = new TestDataProducer(driver);
        var block = new ProduceBlock(INPUT_TOPIC, "string", "nonexistent:FakeSchema",
                List.of(new TestMessage("k1", "v1", null)),
                null, null);

        assertThrows(TestDefinitionException.class,
                () -> producer.produce(List.of(block)));
    }
}
