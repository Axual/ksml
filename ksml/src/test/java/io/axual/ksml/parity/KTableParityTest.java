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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;

/**
 * Parity cases for KTable operations, table-table joins and foreign-key joins.
 */
@ExtendWith(KSMLTestExtension.class)
class KTableParityTest {
    private static final Consumed<String, String> CONSUMED = Consumed.with(Serdes.String(), Serdes.String());
    private static final Produced<String, String> PRODUCED = Produced.with(Serdes.String(), Serdes.String());
    private static final Produced<String, Long> PRODUCED_LONG = Produced.with(Serdes.String(), Serdes.Long());

    private static boolean isBig(String value) {
        return value != null && Json.num(value, "amount") > 10;
    }

    @Test
    void c18TableFilter() {
        ParityCase.of("C18", "KTable filter with and without a store: an update that stops matching sends a tombstone")
                .input("t_in").output("out_store").output("out_plain").store("filter_store")
                .reference(builder -> {
                    final var table = builder.table("t_in", CONSUMED);
                    table.filter((k, v) -> isBig(v), Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("filter_store")
                                    .withKeySerde(Serdes.String()).withValueSerde(Serdes.String()))
                            .toStream().to("out_store", PRODUCED);
                    table.filter((k, v) -> isBig(v)).toStream().to("out_plain", PRODUCED);
                })
                .ksml("parity/c18-table-filter.yaml")
                .pipe("t_in", "k1", "{\"amount\":20}", 1000)
                .pipe("t_in", "k1", "{\"amount\":5}", 2000)
                .pipe("t_in", "k2", "{\"amount\":3}", 3000)
                .pipe("t_in", "k3", "{\"amount\":50}", 4000)
                .pipe("t_in", "k3", null, 5000)
                .pipe("t_in", "k4", null, 6000)
                .knownDifference("F16", "KSML always materializes a source table, so KTable.filter sends fewer tombstones (by design)")
                .run();
    }

    @Test
    void c18bTableFilterOnMaterializedSource() {
        ParityCase.of("C18b", "KTable filter when the Kafka Streams source table is materialized too (KSML always does this)")
                .input("t_in").output("out_store").output("out_plain").store("filter_store")
                .reference(builder -> {
                    final var table = builder.table("t_in", CONSUMED, Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("t_in")
                            .withKeySerde(Serdes.String()).withValueSerde(Serdes.String()));
                    table.filter((k, v) -> isBig(v), Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("filter_store")
                                    .withKeySerde(Serdes.String()).withValueSerde(Serdes.String()))
                            .toStream().to("out_store", PRODUCED);
                    table.filter((k, v) -> isBig(v)).toStream().to("out_plain", PRODUCED);
                })
                .ksml("parity/c18-table-filter.yaml")
                .pipe("t_in", "k1", "{\"amount\":20}", 1000)
                .pipe("t_in", "k1", "{\"amount\":5}", 2000)
                .pipe("t_in", "k2", "{\"amount\":3}", 3000)
                .pipe("t_in", "k3", "{\"amount\":50}", 4000)
                .pipe("t_in", "k3", null, 5000)
                .pipe("t_in", "k4", null, 6000)
                .run();
    }

    @Test
    void c19TableMapValues() {
        ParityCase.of("C19", "KTable mapValues: a tombstone stays a tombstone")
                .input("t_in").output("out")
                .reference(builder -> builder.table("t_in", CONSUMED)
                        .mapValues(v -> v == null ? null : Json.str(Json.obj().put("double", Json.num(v, "amount") * 2)))
                        .toStream().to("out", PRODUCED))
                .ksml("parity/c19-table-map-values.yaml")
                .pipe("t_in", "k1", "{\"amount\":5}", 1000)
                .pipe("t_in", "k1", "{\"amount\":6}", 2000)
                .pipe("t_in", "k2", "{\"amount\":1}", 3000)
                .pipe("t_in", "k1", null, 4000)
                .pipe("t_in", "k3", null, 5000)
                .run();
    }

    @Test
    void c20TableToStreamWithMapper() {
        ParityCase.of("C20", "KTable toStream with a key mapper, also for tombstones")
                .input("t_in").output("out")
                .reference(builder -> builder.table("t_in", CONSUMED)
                        .toStream((k, v) -> k + "-x").to("out", PRODUCED))
                .ksml("parity/c20-table-to-stream-mapper.yaml")
                .pipe("t_in", "k1", "{\"amount\":5}", 1000)
                .pipe("t_in", "k1", null, 2000)
                .run();
    }

    private static void tableGroupByReference(StreamsBuilder builder) {
        final var users = builder.table("t_users", CONSUMED);
        final var grouped = Grouped.with(Serdes.String(), Serdes.String());
        users.groupBy((k, v) -> KeyValue.pair(Json.text(v, "region"), v), grouped)
                .count(Materialized.with(Serdes.String(), Serdes.Long()))
                .toStream().to("out_count", PRODUCED_LONG);
        users.groupBy((k, v) -> KeyValue.pair(Json.text(v, "region"), v), grouped)
                .aggregate(() -> 0L, (k, v, agg) -> agg + Json.num(v, "amount"), (k, v, agg) -> agg - Json.num(v, "amount"),
                        Materialized.with(Serdes.String(), Serdes.Long()))
                .toStream().to("out_sum", PRODUCED_LONG);
    }

    private static ParityCase tableGroupByInput(ParityCase parityCase) {
        return parityCase
                .pipe("t_users", "u1", "{\"region\":\"eu\",\"amount\":10}", 1000)
                .pipe("t_users", "u2", "{\"region\":\"eu\",\"amount\":5}", 2000)
                .pipe("t_users", "u1", "{\"region\":\"us\",\"amount\":10}", 3000)
                .pipe("t_users", "u2", null, 4000)
                .pipe("t_users", "u3", "{\"region\":\"us\",\"amount\":1}", 5000);
    }

    @Test
    void c21TableGroupByWithSubtractor() {
        tableGroupByInput(ParityCase.of("C21", "KTable groupBy + count and aggregate with subtractor: a value moves to a new key, a delete")
                .input("t_users").output("out_count", Format.STRING, Format.LONG).output("out_sum", Format.STRING, Format.LONG)
                .reference(KTableParityTest::tableGroupByReference)
                .ksml("parity/c21-table-group-by.yaml"))
                .knownDifference("F01", "a new key is seen as an update of a DataNull value, the subtractor gets None and the app stops")
                .run();
    }

    @Test
    void c21bTableGroupByWithNoneGuards() {
        tableGroupByInput(ParityCase.of("C21b", "KTable groupBy + count and aggregate, every KSML function guards against None")
                .input("t_users").output("out_count", Format.STRING, Format.LONG).output("out_sum", Format.STRING, Format.LONG)
                .reference(KTableParityTest::tableGroupByReference)
                .ksml("parity/c21b-table-group-by-guarded.yaml"))
                .knownDifference("F05", "the subtractor value parameter is typed as the aggregate type; the count is also wrong (F01)")
                .run();
    }

    @Test
    void c22SuppressUntilTimeLimit() {
        ParityCase.of("C22", "KTable suppress untilTimeLimit with no buffer settings (unbounded in Kafka Streams)")
                .input("t_in").output("out")
                .reference(builder -> builder.table("t_in", CONSUMED)
                        .suppress(Suppressed.untilTimeLimit(Duration.ofSeconds(10), Suppressed.BufferConfig.unbounded()))
                        .toStream().to("out", PRODUCED))
                .ksml("parity/c22-table-suppress-time-limit.yaml")
                .pipe("t_in", "k1", "{\"v\":1}", 1000)
                .pipe("t_in", "k1", "{\"v\":2}", 2000)
                .pipe("t_in", "k2", "{\"v\":3}", 3000)
                .pipe("t_in", "k9", "{\"v\":9}", 20000)
                .run();
    }

    @Test
    void c22bSuppressShutdownWhenFull() {
        ParityCase.of("C22b", "suppress untilTimeLimit with maxRecords 1 and bufferFullStrategy shutdownWhenFull")
                .input("t_in").output("out")
                .reference(builder -> builder.table("t_in", CONSUMED)
                        .suppress(Suppressed.untilTimeLimit(Duration.ofSeconds(10), Suppressed.BufferConfig.maxRecords(1).shutDownWhenFull()))
                        .toStream().to("out", PRODUCED))
                .ksml("parity/c22b-suppress-shutdown-when-full.yaml")
                .pipe("t_in", "k1", "{\"v\":1}", 1000)
                .pipe("t_in", "k2", "{\"v\":2}", 2000)
                .pipe("t_in", "k9", "{\"v\":9}", 20000)
                .knownDifference("F10", "bufferFullStrategy shutdownWhenFull is ignored for untilTimeLimit, KSML emits early")
                .run();
    }

    private static final ValueJoiner<String, String, String> TABLE_JOINER = (l, r) -> {
        final var result = Json.obj();
        if (l == null) result.putNull("left");
        else result.set("left", Json.obj(l).get("v"));
        if (r == null) result.putNull("right");
        else result.set("right", Json.obj(r).get("v"));
        return Json.str(result);
    };

    @Test
    void c35TableTableJoins() {
        ParityCase.of("C35", "table-table join, leftJoin and outerJoin with tombstones on both sides")
                .input("t_left").input("t_right").output("out_join").output("out_left").output("out_outer")
                .reference(builder -> {
                    final var left = builder.table("t_left", CONSUMED);
                    final var right = builder.table("t_right", CONSUMED);
                    left.join(right, TABLE_JOINER).toStream().to("out_join", PRODUCED);
                    left.leftJoin(right, TABLE_JOINER).toStream().to("out_left", PRODUCED);
                    left.outerJoin(right, TABLE_JOINER).toStream().to("out_outer", PRODUCED);
                })
                .ksml("parity/c35-table-table-join.yaml")
                .pipe("t_left", "k1", "{\"v\":1}", 1000)
                .pipe("t_right", "k1", "{\"v\":10}", 2000)
                .pipe("t_right", "k2", "{\"v\":20}", 3000)
                .pipe("t_left", "k1", null, 4000)
                .pipe("t_right", "k2", null, 5000)
                .pipe("t_left", "k3", "{\"v\":3}", 6000)
                .knownDifference("F01", "a table delete is joined as a None value instead of giving a tombstone")
                .run();
    }

    private static final ValueJoiner<String, String, String> ENRICH = (order, customer) -> {
        final var result = Json.obj();
        result.put("amount", Json.num(order, "amount"));
        if (customer == null) result.putNull("name");
        else result.put("name", Json.text(customer, "name"));
        return Json.str(result);
    };

    @Test
    void c36ForeignKeyJoin() {
        ParityCase.of("C36", "foreign-key join and leftJoin: the foreign key changes, the other side is deleted, a null foreign key")
                .input("t_orders").input("t_customers").output("out_fk").output("out_fk_left")
                .reference(builder -> {
                    final var orders = builder.table("t_orders", CONSUMED);
                    final var customers = builder.table("t_customers", CONSUMED);
                    orders.join(customers, (String v) -> Json.text(v, "customer"), ENRICH).toStream().to("out_fk", PRODUCED);
                    orders.leftJoin(customers, (String v) -> Json.text(v, "customer"), ENRICH).toStream().to("out_fk_left", PRODUCED);
                })
                .ksml("parity/c36-fk-join.yaml")
                .pipe("t_customers", "c1", "{\"name\":\"Ann\"}", 1000)
                .pipe("t_orders", "o1", "{\"customer\":\"c1\",\"amount\":10}", 2000)
                .pipe("t_orders", "o2", "{\"customer\":\"c2\",\"amount\":5}", 3000)
                .pipe("t_customers", "c2", "{\"name\":\"Bob\"}", 4000)
                .pipe("t_orders", "o1", "{\"customer\":\"c2\",\"amount\":11}", 5000)
                .pipe("t_customers", "c1", null, 6000)
                .pipe("t_customers", "c2", null, 7000)
                .pipe("t_orders", "o3", "{\"amount\":7}", 8000)
                .pipe("t_orders", "o2", null, 9000)
                .knownDifference("F01", "a tombstone on the orders table reaches the foreign-key extractor as None, so the app stops")
                .run();
    }

    @Test
    void c36bForeignKeyJoinWithOtherKeyType() {
        ParityCase.of("C36b", "foreign-key join where the other table has a different key type (long)")
                .input("t_orders").input("t_customers", Format.LONG, Format.JSON).output("out_fk")
                .reference(builder -> {
                    final var orders = builder.table("t_orders", CONSUMED);
                    final var customers = builder.table("t_customers", Consumed.with(Serdes.Long(), Serdes.String()));
                    orders.join(customers, (String v) -> Json.num(v, "customer"), ENRICH).toStream().to("out_fk", PRODUCED);
                })
                .ksml("parity/c36b-fk-join-other-key-type.yaml")
                .pipe("t_customers", "1", "{\"name\":\"Ann\"}", 1000)
                .pipe("t_orders", "o1", "{\"customer\":1,\"amount\":10}", 2000)
                .run();
    }

    @Test
    void c36cStreamToTableThenForeignKeyJoin() {
        // Kafka Streams has no foreign-key join for a stream, so the stream becomes a table first. KSML rejects a
        // foreignKeyExtractor on a stream join at startup (F04, see usability probe u13). The tombstone path is in C36.
        ParityCase.of("C36c", "stream to table, then a foreign-key join (as in the KSML docs example): null key, late record")
                .input("s_orders").input("t_customers").output("out")
                .reference(builder -> {
                    final var customers = builder.table("t_customers", CONSUMED);
                    builder.stream("s_orders", CONSUMED)
                            .toTable(Materialized.with(Serdes.String(), Serdes.String()))
                            .join(customers, (String v) -> Json.text(v, "customer"), ENRICH)
                            .toStream()
                            .to("out", PRODUCED);
                })
                .ksml("parity/c36c-stream-to-table-fk-join.yaml")
                .pipe("t_customers", "c1", "{\"name\":\"Ann\"}", 1000)
                .pipe("s_orders", "o1", "{\"customer\":\"c1\",\"amount\":10}", 2000)
                .pipe("s_orders", "o2", "{\"customer\":\"c2\",\"amount\":5}", 3000)
                .pipe("t_customers", "c2", "{\"name\":\"Bob\"}", 4000)
                .pipe("s_orders", "o1", "{\"customer\":\"c2\",\"amount\":11}", 5000)
                .pipe("s_orders", null, "{\"customer\":\"c1\",\"amount\":7}", 6000)
                .pipe("s_orders", "o3", "{\"customer\":\"c1\",\"amount\":3}", 1500)
                .run();
    }

    @Test
    void c38ForeignKeyExtractorWithKey() {
        ParityCase.of("C38", "foreign-key extractor that reads the key (BiFunction, Kafka Streams 4.x)")
                .input("t_orders").input("t_customers").output("out_fk")
                .reference(builder -> {
                    final var orders = builder.table("t_orders", CONSUMED);
                    final var customers = builder.table("t_customers", CONSUMED);
                    orders.join(customers, (String k, String v) -> k.split("-")[0], ENRICH).toStream().to("out_fk", PRODUCED);
                })
                .ksml("parity/c38-fk-extractor-with-key.yaml")
                .pipe("t_customers", "c1", "{\"name\":\"Ann\"}", 1000)
                .pipe("t_orders", "c1-o1", "{\"amount\":10}", 2000)
                .knownDifference("F21", "no foreign-key extractor that reads the key")
                .run();
    }
}
