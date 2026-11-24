package io.axual.ksml.parity;

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

import io.axual.ksml.testutil.KSMLTestExtension;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TableJoined;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Cases added for Kafka Streams 4.4.0: the mapped key in stream-globalTable joins (KIP-1340), partitioners that
 * read headers (KIP-1321), headers-aware DSL stores ({@code dsl.store.format=HEADERS}, KIP-1285 and the 4.4
 * suppress buffer), and the streams rebalance protocol with static membership.
 */
@ExtendWith(KSMLTestExtension.class)
class Kafka44ParityTest {
    private static final Consumed<String, String> CONSUMED = Consumed.with(Serdes.String(), Serdes.String());
    private static final Produced<String, String> PRODUCED = Produced.with(Serdes.String(), Serdes.String());
    private static final Produced<String, Long> PRODUCED_LONG = Produced.with(Serdes.String(), Serdes.Long());
    private static final Grouped<String, String> GROUPED = Grouped.with(Serdes.String(), Serdes.String());
    private static final String HEADERS_FORMAT = "HEADERS";

    private static String header(Headers headers, String key) {
        final var header = headers == null ? null : headers.lastHeader(key);
        return header == null || header.value() == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    /**
     * A partitioner that implements the Kafka 4.4 method with headers. The old method is still abstract in 4.4, so it
     * has to be implemented as well; it passes an empty header list on, like KSML does.
     */
    private static <V> StreamPartitioner<String, V> byRegionHeader(boolean lastPartitionForEu) {
        return new StreamPartitioner<>() {
            @Override
            @SuppressWarnings("removal")
            public Optional<Set<Integer>> partitions(String topic, String key, V value, int numPartitions) {
                return partitions(topic, key, value, new RecordHeaders(), numPartitions);
            }

            @Override
            public Optional<Set<Integer>> partitions(String topic, String key, V value, Headers headers, int numPartitions) {
                final var region = header(headers, "region");
                if (lastPartitionForEu) return Optional.of(Set.of("eu".equals(region) ? numPartitions - 1 : 0));
                if ("drop".equals(region)) return Optional.of(Set.of());
                return Optional.of(Set.of("eu".equals(region) ? 0 : "us".equals(region) ? 1 : 2));
            }
        };
    }

    private static String mappedJoined(String key, String mappedKey, String order, String customer) {
        final var result = Json.obj();
        result.put("key", key);
        if (mappedKey == null) result.putNull("mappedKey");
        else result.put("mappedKey", mappedKey);
        result.put("amount", Json.num(order, "amount"));
        if (customer == null) result.putNull("name");
        else result.put("name", Json.text(customer, "name"));
        return Json.str(result);
    }

    @Test
    void c34bGlobalTableJoinWithMappedKey() {
        ParityCase.of("C34b", "stream-globalTable join and leftJoin: the joiner gets the stream key and the mapped key (KIP-1340)")
                .input("s_in").input("g_customers").global("g_customers").output("out_join").output("out_left")
                .reference(builder -> {
                    final var customers = builder.globalTable("g_customers", CONSUMED);
                    final var orders = builder.stream("s_in", CONSUMED);
                    orders.join(customers, (k, v) -> Json.text(v, "customer"), Kafka44ParityTest::mappedJoined).to("out_join", PRODUCED);
                    orders.leftJoin(customers, (k, v) -> Json.text(v, "customer"), Kafka44ParityTest::mappedJoined).to("out_left", PRODUCED);
                })
                .ksml("parity/c34b-global-table-join-mapped-key.yaml")
                .pipe("g_customers", "c1", "{\"name\":\"Ann\"}", 1000)
                .pipe("s_in", "o1", "{\"customer\":\"c1\",\"amount\":10}", 2000)
                .pipe("s_in", "o2", "{\"customer\":\"c2\",\"amount\":5}", 3000)
                .pipe("g_customers", "c2", "{\"name\":\"Bob\"}", 4000)
                .pipe("s_in", "o3", "{\"customer\":\"c2\",\"amount\":7}", 5000)
                .check();
    }

    @Test
    void c45bToWithHeaderPartitioner() {
        ParityCase.of("C45b", "to with a partitioner that reads a header, 3 partitions; an empty partition set drops the record (KIP-1321)")
                .input("in").output("out").partitions("out", 3).compareHeaders()
                .dockerOnly()
                .reference(builder -> builder.stream("in", CONSUMED)
                        .to("out", Produced.with(Serdes.String(), Serdes.String()).withStreamPartitioner(byRegionHeader(false))))
                .ksml("parity/c45b-to-header-partitioner.yaml")
                .pipe("in", "k1", "{\"n\":1}", 1000, Map.of("region", "eu"))
                .pipe("in", "k2", "{\"n\":2}", 2000, Map.of("region", "us"))
                .pipe("in", "k3", "{\"n\":3}", 3000, Map.of("region", "drop"))
                .pipe("in", "k4", "{\"n\":4}", 4000)
                .pipe("in", "k5", "{\"n\":5}", 5000, Map.of("region", "eu"))
                .check();
    }

    @Test
    void c46bRepartitionWithHeaderPartitioner() {
        ParityCase.of("C46b", "repartition with a partitioner that reads a header, output on 3 partitions (KIP-1321)")
                .input("in").output("out").partitions("out", 3).compareHeaders()
                // Each repartition partition has its own task, and the tasks run in any order
                .anyOrderInDocker()
                .dockerOnly()
                .reference(builder -> builder.stream("in", CONSUMED)
                        .repartition(Repartitioned.with(Serdes.String(), Serdes.String()).withNumberOfPartitions(3)
                                .withStreamPartitioner(byRegionHeader(false)))
                        .peek((k, v) -> System.out.println("after repartition " + k + " " + v))
                        .to("out", PRODUCED))
                .ksml("parity/c46b-repartition-header-partitioner.yaml")
                .pipe("in", "k1", "{\"n\":1}", 1000, Map.of("region", "eu"))
                .pipe("in", "k2", "{\"n\":2}", 2000, Map.of("region", "us"))
                .pipe("in", "k3", "{\"n\":3}", 3000, Map.of("region", "drop"))
                .pipe("in", "k4", "{\"n\":4}", 4000)
                .pipe("in", "k5", "{\"n\":5}", 5000, Map.of("region", "eu"))
                .check();
    }

    @Test
    void c36dForeignKeyJoinWithHeaderPartitioners() {
        ParityCase.of("C36d", "foreign-key join with a partitioner and otherPartitioner that read a header (KIP-1321)")
                .input("t_orders").input("t_customers").output("out_fk")
                .reference(builder -> {
                    final var orders = builder.table("t_orders", CONSUMED);
                    final var customers = builder.table("t_customers", CONSUMED);
                    orders.join(customers, (String v) -> Json.text(v, "customer"), (order, customer) -> {
                                final var result = Json.obj();
                                result.put("amount", Json.num(order, "amount"));
                                if (customer == null) result.putNull("name");
                                else result.put("name", Json.text(customer, "name"));
                                return Json.str(result);
                            }, TableJoined.with(byRegionHeader(true), byRegionHeader(true)))
                            .toStream().to("out_fk", PRODUCED);
                })
                .ksml("parity/c36d-fk-join-header-partitioners.yaml")
                .pipe("t_customers", "c1", "{\"name\":\"Ann\"}", 1000, Map.of("region", "eu"))
                .pipe("t_orders", "o1", "{\"customer\":\"c1\",\"amount\":10}", 2000, Map.of("region", "eu"))
                .pipe("t_orders", "o2", "{\"customer\":\"c1\",\"amount\":5}", 3000)
                .check();
    }

    @Test
    void c49HeadersFormatCountAndAggregate() {
        ParityCase.of("C49", "dsl.store.format=HEADERS: groupByKey + count (no KSML store) and aggregate (declared KSML store), headers compared")
                .input("in").output("out_count", Format.STRING, Format.LONG).output("out_sum").compareHeaders()
                .config("dsl.store.format", HEADERS_FORMAT)
                .reference(builder -> {
                    final var input = builder.stream("in", CONSUMED).groupByKey(GROUPED);
                    input.count(Materialized.with(Serdes.String(), Serdes.Long())).toStream().to("out_count", PRODUCED_LONG);
                    input.aggregate(() -> "{\"sum\":0}",
                                    (k, v, agg) -> "{\"sum\":" + (Json.num(agg, "sum") + Json.num(v, "amount")) + "}",
                                    Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("sum_store")
                                            .withKeySerde(Serdes.String()).withValueSerde(Serdes.String()))
                            .toStream().to("out_sum", PRODUCED);
                })
                .ksml("parity/c49-headers-format-count-aggregate.yaml")
                .pipe("in", "a", "{\"amount\":1}", 1000, Map.of("trace", "t-1"))
                .pipe("in", "b", "{\"amount\":2}", 2000, Map.of("trace", "t-2"))
                .pipe("in", "a", "{\"amount\":3}", 3000, Map.of("trace", "t-3"))
                .pipe("in", "a", "{\"amount\":4}", 4000)
                .check();
    }

    @Test
    void c50HeadersFormatSuppress() {
        ParityCase.of("C50", "dsl.store.format=HEADERS: KTable suppress untilTimeLimit, a row updated before it is sent, headers compared")
                .input("t_in").output("out").compareHeaders()
                .config("dsl.store.format", HEADERS_FORMAT)
                .reference(builder -> builder.table("t_in", CONSUMED)
                        .suppress(Suppressed.untilTimeLimit(Duration.ofSeconds(10), Suppressed.BufferConfig.unbounded()))
                        .toStream().to("out", PRODUCED))
                .ksml("parity/c50-headers-format-suppress.yaml")
                .pipe("t_in", "k1", "{\"v\":1}", 1000, Map.of("trace", "t-1"))
                .pipe("t_in", "k1", "{\"v\":2}", 2000, Map.of("trace", "t-2"))
                .pipe("t_in", "k2", "{\"v\":3}", 3000, Map.of("trace", "t-3"))
                .pipe("t_in", "k9", "{\"v\":9}", 20000, Map.of("trace", "t-9"))
                .check();
    }

    @Test
    void c50bHeadersFormatLeftJoin() {
        ParityCase.of("C50b", "dsl.store.format=HEADERS: stream-stream join and leftJoin, left-only results come from a buffer, headers compared")
                .input("left").input("right").output("out_join").output("out_left").compareHeaders()
                .config("dsl.store.format", HEADERS_FORMAT)
                .reference(builder -> {
                    final var joined = StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String());
                    final var window = JoinWindows.ofTimeDifferenceAndGrace(Duration.ofSeconds(10), Duration.ZERO);
                    final var left = builder.stream("left", CONSUMED);
                    final var right = builder.stream("right", CONSUMED);
                    left.join(right, StreamJoinParityTest::joined, window, joined).to("out_join", PRODUCED);
                    left.leftJoin(right, StreamJoinParityTest::joined, window, joined).to("out_left", PRODUCED);
                })
                .ksml("parity/c50b-headers-format-left-join.yaml")
                .pipe("left", "k1", "{\"v\":1}", 1000, Map.of("trace", "l-1"))
                .pipe("right", "k1", "{\"v\":2}", 5000, Map.of("trace", "r-2"))
                .pipe("left", "k2", "{\"v\":4}", 3000, Map.of("trace", "l-4"))
                .pipe("left", "k9", "{\"v\":9}", 40000, Map.of("trace", "l-9"))
                .check();
    }

    private static void countReference(StreamsBuilder builder) {
        builder.stream("in", CONSUMED)
                .groupByKey(GROUPED)
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("count_store")
                        .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()))
                .toStream()
                .to("out", PRODUCED_LONG);
    }

    private static ParityCase countAcrossRestart(String id, String title) {
        return ParityCase.of(id, title)
                .input("in").output("out", Format.STRING, Format.LONG)
                .dockerOnly()
                .config("group.protocol", "streams")
                .reference(Kafka44ParityTest::countReference)
                .ksml("parity/c44b-count-store-like-kafka-streams.yaml")
                .pipe("in", "a", "{\"n\":1}", 1000)
                .pipe("in", "b", "{\"n\":2}", 2000)
                .pipe("in", "a", "{\"n\":3}", 3000)
                .restartAfter(3)
                .pipe("in", "a", "{\"n\":4}", 4000)
                .pipe("in", "b", "{\"n\":5}", 5000);
    }

    @Test
    void c52StreamsProtocolCountAcrossRestart() {
        countAcrossRestart("C52", "group.protocol=streams: count with a persistent, logged store survives a restart").check();
    }

    @Test
    void c53StreamsProtocolStaticMembership() {
        countAcrossRestart("C53", "group.protocol=streams with static membership (group.instance.id): count survives a restart")
                .config("group.instance.id", "parity-static-1")
                .check();
    }
}
