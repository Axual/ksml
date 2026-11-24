package io.axual.ksml.operation;

/*-
 * ========================LICENSE_START=================================
 * KSML
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

import com.google.common.collect.ImmutableMap;
import io.axual.ksml.TopologyGenerator;
import io.axual.ksml.data.notation.binary.BinaryNotation;
import io.axual.ksml.data.notation.json.JsonNotation;
import io.axual.ksml.definition.parser.TopologyDefinitionParser;
import io.axual.ksml.execution.ExecutionContext;
import io.axual.ksml.generator.YAMLObjectMapper;
import io.axual.ksml.parser.ParseNode;
import io.axual.ksml.type.UserType;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

/** Builds topologies with a punctuate operation and runs them with the TopologyTestDriver. */
class PunctuateOperationTest {
    private static final Instant START = Instant.ofEpochMilli(1_700_000_000_000L);
    private static final String STREAMS = """
            streams:
              input:
                topic: input
                keyType: string
                valueType: long
              output:
                topic: output
                keyType: string
                valueType: long
            """;

    private TopologyTestDriver driver;
    private TestInputTopic<String, Long> input;
    private TestOutputTopic<String, Long> output;

    @BeforeAll
    static void registerNotations() {
        final var jsonNotation = new JsonNotation();
        ExecutionContext.INSTANCE.notationLibrary().register(UserType.DEFAULT_NOTATION, new BinaryNotation(jsonNotation::serde));
        ExecutionContext.INSTANCE.notationLibrary().register(JsonNotation.NOTATION_NAME, jsonNotation);
    }

    @AfterEach
    void closeDriver() {
        if (driver != null) driver.close();
    }

    private static Topology topology(String yaml) {
        final var definition = YAMLObjectMapper.INSTANCE.readValue(yaml, JsonNode.class);
        final var definitions = ImmutableMap.of("definition", new TopologyDefinitionParser("test").parse(ParseNode.fromRoot(definition, "test")));
        return new TopologyGenerator("punctuate.app", null, null).create(new StreamsBuilder(), definitions);
    }

    private void start(String yaml) {
        final var properties = new Properties();
        properties.put("application.id", "punctuate.app");
        driver = new TopologyTestDriver(topology(yaml), properties, START);
        input = driver.createInputTopic("input", new StringSerializer(), new LongSerializer(), START, Duration.ZERO);
        output = driver.createOutputTopic("output", new StringDeserializer(), new LongDeserializer());
    }

    private static String pipeline(String via) {
        return STREAMS + """
                pipelines:
                  main:
                    from: input
                    via:
                %s
                    to: output
                """.formatted(via.indent(6));
    }

    @Test
    @DisplayName("records pass through unchanged, with key, value, timestamp and headers")
    void passThrough() {
        start(pipeline("""
                - type: punctuate
                  interval: 1m
                  punctuationType: wallClockTime
                  punctuator:
                    code: pass
                """));
        final var headers = new RecordHeaders().add("h", "v".getBytes(StandardCharsets.UTF_8));
        input.pipeInput(new TestRecord<>("a", 1L, headers, START.plusSeconds(5)));
        final var result = output.readRecord();
        assertThat(result.key()).isEqualTo("a");
        assertThat(result.value()).isEqualTo(1L);
        assertThat(result.getRecordTime()).isEqualTo(START.plusSeconds(5));
        assertThat(result.headers()).isEqualTo(headers);
        assertThat(output.isEmpty()).isTrue();
    }

    @Test
    @DisplayName("a wall-clock punctuator runs without input, once per interval, and forwards in call order with the punctuation timestamp")
    void wallClockWithoutInput() {
        start(pipeline("""
                - type: punctuate
                  interval: 1m
                  punctuationType: wallClockTime
                  punctuator:
                    code: |
                      context.forward("a", 10)
                      context.forward("b", 20)
                """));
        driver.advanceWallClockTime(Duration.ofMinutes(3));
        final var records = output.readRecordsToList();
        assertThat(records).extracting(TestRecord::key, TestRecord::value).containsExactly(
                tuple("a", 10L), tuple("b", 20L));
        assertThat(records.get(0).getRecordTime()).isEqualTo(START.plus(Duration.ofMinutes(3)));
    }

    private static String wallClock(String code) {
        return """
                - type: punctuate
                  name: flush
                  interval: 1m
                  punctuationType: wallClockTime
                  punctuator:
                    code: |
                %s
                """.formatted(code.indent(6));
    }

    private void tick() {
        driver.advanceWallClockTime(Duration.ofMinutes(1));
    }

    @Test
    @DisplayName("a wall-clock punctuator is called once per interval while no records arrive")
    void wallClockOncePerInterval() {
        start(pipeline(wallClock("context.forward('tick', timestamp)")));
        for (var minute = 0; minute < 3; minute++) tick();
        assertThat(output.readValuesToList()).containsExactly(
                START.plus(Duration.ofMinutes(1)).toEpochMilli(),
                START.plus(Duration.ofMinutes(2)).toEpochMilli(),
                START.plus(Duration.ofMinutes(3)).toEpochMilli());
    }

    @Test
    @DisplayName("a stream-time punctuator is called with the first record and when stream time advances by the interval, never without input")
    void streamTime() {
        start(pipeline("""
                - type: punctuate
                  interval: 1m
                  punctuationType: streamTime
                  punctuator:
                    code: context.forward("tick", timestamp)
                """));
        driver.advanceWallClockTime(Duration.ofMinutes(5));
        assertThat(output.isEmpty()).isTrue();
        input.pipeInput("a", 0L, START);
        input.pipeInput("a", 30L, START.plusSeconds(30));
        input.pipeInput("a", 61L, START.plusSeconds(61));
        assertThat(output.readKeyValuesToList()).filteredOn(kv -> kv.key.equals("tick"))
                .extracting(kv -> kv.value).containsExactly(START.toEpochMilli(), START.plusSeconds(61).toEpochMilli());
    }

    @Test
    @DisplayName("forward sets an explicit timestamp and headers")
    void timestampAndHeaders() {
        start(pipeline(wallClock("context.forward('a', 10, timestamp=1700000000000, headers=[{'key': 'source', 'value': 'flush'}])")));
        tick();
        final var result = output.readRecord();
        assertThat(result.getRecordTime()).isEqualTo(Instant.ofEpochMilli(1700000000000L));
        assertThat(result.headers()).isEqualTo(new RecordHeaders().add("source", "flush".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    @DisplayName("records forwarded by the punctuator pass through a later filter like input records")
    void filterAfterPunctuate() {
        start(pipeline(wallClock("""
                context.forward('keep', 1)
                context.forward('drop', 2)
                """) + """
                - type: filter
                  if:
                    expression: key != 'drop'
                """));
        tick();
        input.pipeInput("drop", 3L);
        input.pipeInput("keep", 4L);
        assertThat(output.readKeyValuesToList()).containsExactly(KeyValue.pair("keep", 1L), KeyValue.pair("keep", 4L));
    }

    private static final String SKIP = wallClock("context.forward('p', 'x', to='enrich')") + """
            - type: transformValue
              name: compute
              mapper:
                expression: str(value)
                resultType: string
            - type: transformValue
              name: enrich
              mapper:
                expression: value + "!"
                resultType: string
            - type: transformValue
              mapper:
                expression: len(value)
                resultType: long
            """;

    @Test
    @DisplayName("forward with to skips the operations before the target; input records reach the target once")
    void forwardToNamedOperation() {
        start(pipeline(SKIP));
        tick();
        input.pipeInput("i", 12L);
        // "x" skips compute and becomes "x!" (2); 12 becomes "12", then "12!" (3)
        assertThat(output.readKeyValuesToList()).containsExactly(KeyValue.pair("p", 2L), KeyValue.pair("i", 3L));
    }

    @Test
    @DisplayName("forward to an unknown or unreachable name raises an error listing the valid names")
    void forwardToUnknownName() {
        start(pipeline(wallClock("context.forward('p', 1, to='nothing')") + """
                - type: transformValue
                  name: compute
                  mapper:
                    expression: value
                    resultType: long
                - type: transformValue
                  mapper:
                    expression: value
                    resultType: long
                - type: groupByKey
                - type: count
                  name: counted
                - type: toStream
                - type: transformValue
                  name: after_table
                  mapper:
                    expression: value
                    resultType: long
                """));
        assertThatThrownBy(this::tick)
                .hasStackTraceContaining("forward can not send to 'nothing', valid names are: compute");
        assertThat(output.isEmpty()).isTrue();
    }

    private static String totalsPunctuator(String code) {
        return """
                stores:
                  totals:
                    type: keyValue
                    keyType: string
                    valueType: long
                    persistent: false
                """ + pipeline("""
                - type: punctuate
                  interval: 1m
                  punctuationType: wallClockTime
                  punctuator:
                    stores: [totals]
                    globalCode: |
                      def get_total(store, key):
                        return store.get(key)
                      def old_style_total(key):
                        return stores["totals"].get(key)
                    code: |
                      totals.put("a", 5)
                %s
                """.formatted(code.indent(6)));
    }

    @Test
    @DisplayName("a globalCode helper can use a store that the function passes to it")
    void globalCodeHelperGetsStore() {
        start(totalsPunctuator("context.forward('a', get_total(totals, 'a'))"));
        tick();
        assertThat(output.readKeyValuesToList()).containsExactly(KeyValue.pair("a", 5L));
    }

    @Test
    @DisplayName("a globalCode helper can read a store in a with block, which closes the iterator")
    void globalCodeHelperUsesWithBlock() {
        start("""
                stores:
                  totals:
                    type: keyValue
                    keyType: string
                    valueType: long
                    persistent: false
                """ + pipeline("""
                - type: punctuate
                  interval: 1m
                  punctuationType: wallClockTime
                  punctuator:
                    stores: [totals]
                    globalCode: |
                      def find_key(store, value):
                        with store.all() as entries:
                          for entry in entries:
                            if entry["value"] == value:
                              return entry["key"]
                        return None
                    code: |
                      totals.put("a", 5)
                      totals.put("b", 7)
                      context.forward(find_key(totals, 7), 7)
                """));
        tick();
        assertThat(output.readKeyValuesToList()).containsExactly(KeyValue.pair("b", 7L));
    }

    @Test
    @DisplayName("a globalCode helper can not read stores: it is no longer a global")
    void globalCodeHelperHasNoStoresGlobal() {
        start(totalsPunctuator("old_style_total('a')"));
        assertThatThrownBy(this::tick).hasStackTraceContaining("NameError: name 'stores' is not defined");
    }

    @Test
    @DisplayName("the punctuator reads, forwards and deletes the entries of a store an earlier operation writes to")
    void readAndClearStore() {
        start("""
                stores:
                  totals:
                    type: keyValue
                    keyType: string
                    valueType: long
                    persistent: false
                """ + pipeline("""
                - type: transformValue
                  mapper:
                    stores: [totals]
                    code: |
                      total = (totals.get(key) or 0) + value
                      totals.put(key, total)
                    expression: value
                    resultType: long
                - type: filter
                  if:
                    expression: False
                - type: punctuate
                  interval: 1m
                  punctuationType: wallClockTime
                  punctuator:
                    stores: [totals]
                    code: |
                      keys = []
                      for e in totals.all():
                        context.forward(e["key"], e["value"])
                        keys.append(e["key"])
                      for k in keys:
                        totals.delete(k)
                """));
        input.pipeInput("a", 1L);
        input.pipeInput("a", 2L);
        input.pipeInput("b", 5L);
        tick();
        assertThat(output.readKeyValuesToList()).containsExactly(KeyValue.pair("a", 3L), KeyValue.pair("b", 5L));
        assertThat(driver.<String, Object>getKeyValueStore("totals").approximateNumEntries()).isZero();
        tick();
        assertThat(output.isEmpty()).isTrue();
    }

    @Test
    @DisplayName("a store named like the injected context is rejected when the topology is built")
    void storeNamedContext() {
        assertThatThrownBy(() -> start("""
                stores:
                  context:
                    type: keyValue
                    keyType: string
                    valueType: long
                    persistent: false
                """ + pipeline("""
                - type: punctuate
                  interval: 1m
                  punctuationType: wallClockTime
                  punctuator:
                    stores: [context]
                    code: |
                      pass
                """)))
                .hasStackTraceContaining("The store \"context\"")
                .hasStackTraceContaining("clashes with the injected variable of the same name, rename the store");
    }

    @Test
    @DisplayName("a write to a timestamped store without a timestamp uses the punctuation timestamp")
    void timestampedStoreWrite() {
        start("""
                stores:
                  last:
                    type: keyValue
                    keyType: string
                    valueType: long
                    timestamped: true
                    persistent: false
                """ + pipeline("""
                - type: punctuate
                  interval: 1m
                  punctuationType: wallClockTime
                  punctuator:
                    stores: [last]
                    code: |
                      last.put("k", 1)
                      context.forward("k", last.getWithTimestamp("k")["timestamp"])
                """));
        tick();
        assertThat(output.readValue()).isEqualTo(START.plus(Duration.ofMinutes(1)).toEpochMilli());
    }

    @Test
    @DisplayName("a forwarded value of the wrong type raises an error, and nothing is forwarded")
    void wrongValueType() {
        start(pipeline(wallClock("context.forward('a', 'ten')")));
        assertThatThrownBy(this::tick)
                .hasStackTraceContaining("punctuate operation")
                .hasStackTraceContaining("forward value for the next operation must be");
        assertThat(output.isEmpty()).isTrue();
    }

    @Test
    @DisplayName("an exception after a forward stops the punctuation; the forwarded record stays forwarded")
    void exceptionAfterForward() {
        start("""
                stores:
                  seen:
                    type: keyValue
                    keyType: string
                    valueType: long
                    persistent: false
                """ + pipeline(wallClock("""
                context.forward('a', 1)
                raise ValueError('boom')
                """) + """
                - type: peek
                  forEach:
                    stores: [seen]
                    code: seen.put(key, value)
                """));
        assertThatThrownBy(this::tick)
                .hasStackTraceContaining("Error in punctuate operation 'test_flush'")
                .hasStackTraceContaining("during the punctuation at timestamp " + START.plus(Duration.ofMinutes(1)).toEpochMilli())
                .hasStackTraceContaining("boom");
        assertThat(driver.<String, Object>getKeyValueStore("seen").get("a")).isNotNull();
    }

    @Test
    @DisplayName("context.commit() and the other context methods work during a punctuation")
    void contextMethods() {
        start(pipeline(wallClock("""
                context.commit()
                context.forward(context.applicationId() + "/" + context.taskId(), context.currentSystemTimeMs() - context.currentStreamTimeMs())
                """)));
        tick();
        final var result = output.readKeyValue();
        assertThat(result.key).isEqualTo("punctuate.app/0_0");
    }

    @Test
    @DisplayName("a context kept after the call raises an error when used, and forwards nothing")
    void contextKeptAfterCall() {
        start(STREAMS + """
                functions:
                  keep:
                    type: punctuator
                    globalCode: kept = []
                    code: |
                      if not kept:
                        kept.append(context)
                      else:
                        kept[0].forward("late", 1)
                pipelines:
                  main:
                    from: input
                    via:
                      - type: punctuate
                        interval: 1m
                        punctuationType: wallClockTime
                        punctuator: keep
                    to: output
                """);
        tick();
        assertThatThrownBy(this::tick).hasStackTraceContaining("The context is no longer valid");
        assertThat(output.isEmpty()).isTrue();
    }

    @Test
    @DisplayName("a returned value is not forwarded")
    void returnedValueIgnored() {
        start(pipeline(wallClock("return ('a', 1)")));
        tick();
        assertThat(output.isEmpty()).isTrue();
    }

    @Test
    @DisplayName("punctuate after a table is rejected at startup")
    void punctuateAfterTable() {
        final var yaml = STREAMS + """
                tables:
                  table:
                    topic: table_topic
                    keyType: string
                    valueType: long
                pipelines:
                  main:
                    from: table
                    via:
                      - type: punctuate
                        interval: 1m
                        punctuationType: wallClockTime
                        punctuator:
                          code: pass
                    to: output
                """;
        assertThatThrownBy(() -> topology(yaml)).hasMessageContaining("Punctuate needs a stream as input");
    }

    @Test
    @DisplayName("punctuate after groupByKey is rejected at startup")
    void punctuateAfterGroupBy() {
        final var yaml = pipeline("""
                - type: groupByKey
                - type: punctuate
                  interval: 1m
                  punctuationType: wallClockTime
                  punctuator:
                    code: pass
                """);
        assertThatThrownBy(() -> topology(yaml)).hasMessageContaining("Punctuate needs a stream as input");
    }

    @Test
    @DisplayName("named stream operations after punctuate get a side path and a merge; others do not")
    void sidePaths() {
        final var description = topology(pipeline(SKIP)).describe().toString();
        assertThat(description).contains(
                "Processor: test_flush-next", "Processor: test_flush-to-test_compute ", "Processor: test_flush-to-test_compute-merge",
                "Processor: test_flush-to-test_enrich ", "Processor: test_flush-to-test_enrich-merge");
        // Only the named operations: two side paths, each with its merge
        assertThat(description.split("Processor: test_flush-to-")).hasSize(5);
    }

    @Test
    @DisplayName("a repartition can be a target, but operations after it get no side path that skips it")
    void noSidePathsPastRepartition() {
        final var description = topology(pipeline(wallClock("pass") + """
                - type: repartition
                  name: spread
                  numberOfPartitions: 3
                - type: peek
                  name: after_repartition
                  forEach:
                    code: pass
                """)).describe();
        assertThat(description.toString())
                .contains("Processor: test_flush-to-test_spread ")
                .doesNotContain("flush-to-test_after_repartition");
        // The input topic and the repartition topic stay in separate sub-topologies
        assertThat(description.subtopologies()).hasSize(2);
    }

    @Test
    @DisplayName("a join after a key change can be a target, but operations after it get no side path that skips it")
    void noSidePathsPastJoinAfterKeyChange() {
        final var description = topology(STREAMS + """
                tables:
                  lookup:
                    topic: lookup
                    keyType: string
                    valueType: long
                """ + pipeline(wallClock("pass") + """
                - type: transformKey
                  mapper:
                    expression: key.upper()
                    resultType: string
                - type: join
                  name: joined
                  table: lookup
                  valueJoiner:
                    expression: value1 + value2
                    resultType: long
                - type: peek
                  name: after_join
                  forEach:
                    code: pass
                """).substring(STREAMS.length())).describe().toString();
        assertThat(description)
                .contains("Processor: test_flush-to-test_joined ")
                .doesNotContain("flush-to-test_after_join");
    }

    @Test
    @DisplayName("groupBy can be a target, but operations after groupBy, aggregate and toStream get no side path")
    void noSidePathsPastGroupByAggregate() {
        final var description = topology(pipeline(wallClock("pass") + """
                - type: groupBy
                  name: grouped
                  mapper:
                    expression: key.upper()
                    resultType: string
                - type: aggregate
                  name: summed
                  store:
                    name: sums
                    type: keyValue
                    persistent: false
                  initializer:
                    expression: 0
                    resultType: long
                  aggregator:
                    expression: aggregatedValue + value
                    resultType: long
                - type: toStream
                  name: summed_stream
                - type: peek
                  name: after_aggregate
                  forEach:
                    code: pass
                """)).describe().toString();
        assertThat(description)
                .contains("Processor: test_flush-to-test_grouped ")
                .doesNotContain("flush-to-test_summed", "flush-to-test_after_aggregate");
    }

    @Test
    @DisplayName("without named operations after it, punctuate adds no extra nodes")
    void noSidePaths() {
        final var description = topology(pipeline(wallClock("pass") + """
                - type: transformValue
                  mapper:
                    expression: value
                    resultType: long
                """)).describe().toString();
        assertThat(description).doesNotContain("flush-next", "flush-to-");
    }

    @Test
    @DisplayName("operations after groupBy or toTable and in other pipelines get no side path")
    void noSidePathsAfterGroupingOrInOtherPipelines() {
        final var description = topology(STREAMS + """
                pipelines:
                  main:
                    from: input
                    via:
                %s
                      - type: groupByKey
                      - type: count
                        name: counted
                      - type: toStream
                        name: back_to_stream
                      - type: toTable
                        name: as_table
                      - type: toStream
                    to: output
                  other:
                    from: input
                    via:
                      - type: peek
                        name: other_peek
                        forEach:
                          code: pass
                    to: output
                """.formatted(wallClock("pass").indent(6))).describe().toString();
        assertThat(description).doesNotContain("flush-to-counted", "flush-to-as_table", "flush-to-other_peek");
    }

    private static final String FULL_OPERATION = """
            - type: punctuate
              interval: 1m
              punctuationType: wallClockTime
              punctuator:
                code: pass
            """;

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"interval", "punctuationType", "punctuator"})
    @DisplayName("a punctuate operation without a required field is rejected, naming the field")
    void missingField(String field) {
        final var operation = FULL_OPERATION.lines()
                .filter(line -> !line.trim().startsWith(field + ":"))
                .filter(line -> !(field.equals("punctuator") && line.trim().startsWith("code:")))
                .reduce("", (a, b) -> a + b + "\n");
        final var yaml = pipeline(operation);
        assertThatThrownBy(() -> topology(yaml)).hasStackTraceContaining("Punctuate operation needs the field '" + field + "'");
    }

    @Test
    @DisplayName("an interval of 0 is rejected")
    void zeroInterval() {
        final var yaml = pipeline(FULL_OPERATION.replace("1m", "0"));
        assertThatThrownBy(() -> topology(yaml)).hasStackTraceContaining("Punctuate interval must be at least 1 millisecond");
    }

    @Test
    @DisplayName("an unknown punctuationType is rejected, listing the valid values")
    void unknownPunctuationType() {
        final var yaml = pipeline(FULL_OPERATION.replace("wallClockTime", "eventTime"));
        assertThatThrownBy(() -> topology(yaml))
                .hasStackTraceContaining("Illegal value for enum PunctuationType: eventTime, valid values are: wallClockTime, streamTime");
    }

    @Test
    @DisplayName("a punctuator that refers to a function of another type is rejected")
    void wrongFunctionType() {
        final var yaml = STREAMS + """
                functions:
                  is_big:
                    type: predicate
                    expression: value > 10
                pipelines:
                  main:
                    from: input
                    via:
                      - type: punctuate
                        interval: 1m
                        punctuationType: wallClockTime
                        punctuator: is_big
                    to: output
                """;
        assertThatThrownBy(() -> topology(yaml)).hasStackTraceContaining("needs a function of type punctuator, but function 'is_big' has type predicate");
    }
}
