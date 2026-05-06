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
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.graalvm.home.Version;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link AssertionRunner}. Requires GraalVM for Python execution.
 */
@EnabledIf(value = "isRunningOnGraalVM", disabledReason = "Requires GraalVM")
class AssertionRunnerTest {

    private static final String INPUT_TOPIC = "input-topic";
    private static final String OUTPUT_TOPIC = "output-topic";
    private static final String STORE_NAME = "test-store";
    private static final String OUTPUT_STREAM = "output";
    private static final String SUITE = "assertion-runner-test";

    private TopologyTestDriver driver;
    private Map<String, StreamDefinition> streams;

    static boolean isRunningOnGraalVM() {
        return Version.getCurrent().isRelease();
    }

    @BeforeEach
    void setUp() {
        var stringSerde = Serdes.String();
        var builder = new StreamsBuilder();

        // Add a key-value state store to the topology
        var storeBuilder = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(STORE_NAME), stringSerde, stringSerde);
        builder.addStateStore(storeBuilder);

        // Pass-through topology that also writes to a state store
        builder.stream(INPUT_TOPIC, Consumed.with(stringSerde, stringSerde))
                .process((ProcessorSupplier<String, String, String, String>) () -> new Processor<>() {
                    private KeyValueStore<String, String> store;
                    private org.apache.kafka.streams.processor.api.ProcessorContext<String, String> ctx;

                    @Override
                    public void init(org.apache.kafka.streams.processor.api.ProcessorContext<String, String> context) {
                        this.ctx = context;
                        this.store = context.getStateStore(STORE_NAME);
                    }

                    @Override
                    public void process(Record<String, String> record) {
                        store.put(record.key(), record.value());
                        ctx.forward(record);
                    }
                }, STORE_NAME)
                .to(OUTPUT_TOPIC, Produced.with(stringSerde, stringSerde));

        driver = new TopologyTestDriver(builder.build());

        streams = Map.of(OUTPUT_STREAM, new StreamDefinition(OUTPUT_TOPIC, "string", "string"));
    }

    @AfterEach
    void tearDown() {
        if (driver != null) {
            driver.close();
        }
    }

    private void produceMessages(String... keyValuePairs) {
        var inputTopic = driver.createInputTopic(INPUT_TOPIC,
                new StringSerializer(), new StringSerializer());
        for (int i = 0; i < keyValuePairs.length; i += 2) {
            inputTopic.pipeInput(keyValuePairs[i], keyValuePairs[i + 1]);
        }
    }

    @Test
    void passingAssertionReturnsPass() {
        produceMessages("k1", "v1", "k2", "v2");

        var runner = new AssertionRunner(driver, streams);
        var block = new AssertBlock(OUTPUT_STREAM, null,
                "assert len(records) == 2\n");

        var result = runner.runAssertions(List.of(block), SUITE, "pass-test");

        assertEquals(TestResult.Status.PASS, result.status());
        assertEquals("pass-test", result.testName());
        assertEquals(SUITE, result.suiteName());
        assertNull(result.message());
    }

    @Test
    void failingAssertionReturnsFail() {
        produceMessages("k1", "v1");

        var runner = new AssertionRunner(driver, streams);
        var block = new AssertBlock(OUTPUT_STREAM, null,
                "assert len(records) == 99, \"expected 99 records\"\n");

        var result = runner.runAssertions(List.of(block), SUITE, "fail-test");

        assertEquals(TestResult.Status.FAIL, result.status());
        assertEquals("fail-test", result.testName());
        assertNotNull(result.message());
        assertTrue(result.message().contains("AssertionError"));
    }

    @Test
    void assertionErrorWithoutMessageReturnsFail() {
        produceMessages("k1", "v1");

        var runner = new AssertionRunner(driver, streams);
        var block = new AssertBlock(OUTPUT_STREAM, null,
                "assert False\n");

        var result = runner.runAssertions(List.of(block), SUITE, "bare-assert");

        assertEquals(TestResult.Status.FAIL, result.status());
        assertTrue(result.message().contains("AssertionError"));
    }

    @Test
    void pythonErrorReturnsError() {
        produceMessages("k1", "v1");

        var runner = new AssertionRunner(driver, streams);
        var block = new AssertBlock(OUTPUT_STREAM, null,
                "x = 1 / 0\n");

        var result = runner.runAssertions(List.of(block), SUITE, "error-test");

        assertEquals(TestResult.Status.ERROR, result.status());
        assertEquals("error-test", result.testName());
        assertNotNull(result.message());
    }

    @Test
    void recordsContainKeyValueAndTimestamp() {
        produceMessages("mykey", "myvalue");

        var runner = new AssertionRunner(driver, streams);
        var block = new AssertBlock(OUTPUT_STREAM, null, """
                assert len(records) == 1
                assert records[0]["key"] == "mykey"
                assert records[0]["value"] == "myvalue"
                assert "timestamp" in records[0]
                """);

        var result = runner.runAssertions(List.of(block), SUITE, "record-fields");

        assertEquals(TestResult.Status.PASS, result.status());
    }

    @Test
    void emptyTopicYieldsEmptyRecords() {
        var runner = new AssertionRunner(driver, streams);
        var block = new AssertBlock(OUTPUT_STREAM, null,
                "assert len(records) == 0\n");

        var result = runner.runAssertions(List.of(block), SUITE, "empty-topic");

        assertEquals(TestResult.Status.PASS, result.status());
    }

    @Test
    void missingStoreReturnsError() {
        var runner = new AssertionRunner(driver, streams);
        var block = new AssertBlock(null, List.of("nonexistent_store"),
                "pass\n");

        var result = runner.runAssertions(List.of(block), SUITE, "missing-store");

        assertEquals(TestResult.Status.ERROR, result.status());
        assertTrue(result.message().contains("nonexistent_store"));
    }

    @Test
    void multipleAssertBlocksStopOnFirstFailure() {
        produceMessages("k1", "v1");

        var runner = new AssertionRunner(driver, streams);
        var passingBlock = new AssertBlock(OUTPUT_STREAM, null,
                "assert len(records) == 1\n");
        var failingBlock = new AssertBlock(OUTPUT_STREAM, null,
                "assert False, \"should fail\"\n");
        var unreachedBlock = new AssertBlock(OUTPUT_STREAM, null,
                "assert False, \"should not run\"\n");

        var result = runner.runAssertions(
                List.of(passingBlock, failingBlock, unreachedBlock), SUITE, "multi-block");

        assertEquals(TestResult.Status.FAIL, result.status());
        assertTrue(result.message().contains("should fail"));
    }

    @Test
    void allBlocksPassReturnsPass() {
        produceMessages("k1", "v1", "k2", "v2");

        var runner = new AssertionRunner(driver, streams);
        var block1 = new AssertBlock(OUTPUT_STREAM, null,
                "assert len(records) >= 1\n");
        var block2 = new AssertBlock(OUTPUT_STREAM, null,
                "assert True\n");

        var result = runner.runAssertions(List.of(block1, block2), SUITE, "all-pass");

        assertEquals(TestResult.Status.PASS, result.status());
    }

    @Test
    void emptyAssertBlockListReturnsPass() {
        var runner = new AssertionRunner(driver, streams);

        var result = runner.runAssertions(List.of(), SUITE, "no-blocks");

        assertEquals(TestResult.Status.PASS, result.status());
    }
}
