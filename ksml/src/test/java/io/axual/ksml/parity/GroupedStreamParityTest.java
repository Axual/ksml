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
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Parity cases for grouping and aggregation of streams.
 */
@ExtendWith(KSMLTestExtension.class)
class GroupedStreamParityTest {
    private static final Consumed<String, String> CONSUMED = Consumed.with(Serdes.String(), Serdes.String());
    private static final Produced<String, String> PRODUCED = Produced.with(Serdes.String(), Serdes.String());
    private static final Produced<String, Long> PRODUCED_LONG = Produced.with(Serdes.String(), Serdes.Long());
    private static final Grouped<String, String> GROUPED = Grouped.with(Serdes.String(), Serdes.String());

    private static String user(String value) {
        return value == null ? null : Json.text(value, "user");
    }

    private static String sumAndCount(String aggregate, String value) {
        final var result = Json.obj();
        result.put("count", Json.num(aggregate, "count") + 1);
        result.put("sum", Json.num(aggregate, "sum") + Json.num(value, "amount"));
        return Json.str(result);
    }

    @Test
    void c03SelectKeyThenGroupByKey() {
        ParityCase.of("C03", "selectKey then groupByKey + count: repartition, null keys are dropped")
                .input("in").output("out", Format.STRING, Format.LONG).compareShape()
                .reference(builder -> builder.stream("in", CONSUMED)
                        .selectKey((k, v) -> user(v))
                        .groupByKey(GROUPED)
                        .count(Materialized.with(Serdes.String(), Serdes.Long()))
                        .toStream()
                        .to("out", PRODUCED_LONG))
                .ksml("parity/c03-select-key-count.yaml")
                .pipe("in", "k1", "{\"user\":\"alice\"}", 1000)
                .pipe("in", "k2", "{\"user\":\"bob\"}", 2000)
                .pipe("in", "k3", "{\"user\":\"alice\"}", 3000)
                .pipe("in", "k4", "{\"other\":1}", 4000)
                .pipe("in", "k5", null, 5000)
                .check();
    }

    @Test
    void c12GroupByKeyCount() {
        ParityCase.of("C12", "groupByKey + count, null key and null value are skipped")
                .input("in").output("out", Format.STRING, Format.LONG).store("count_store")
                .reference(builder -> builder.stream("in", CONSUMED)
                        .groupByKey(GROUPED)
                        .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("count_store")
                                .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()))
                        .toStream()
                        .to("out", PRODUCED_LONG))
                .ksml("parity/c12-count.yaml")
                .pipe("in", "a", "{\"n\":1}", 1000)
                .pipe("in", "b", "{\"n\":2}", 2000)
                .pipe("in", "a", "{\"n\":3}", 3000)
                .pipe("in", null, "{\"n\":4}", 4000)
                .pipe("in", "a", null, 5000)
                .pipe("in", "b", "{\"n\":5}", 6000)
                .knownDifference("F01", "a tombstone reaches Kafka Streams as DataNull, not null, so count counts it")
                .check();
    }

    @Test
    void c13GroupByCount() {
        ParityCase.of("C13", "groupBy + count: the mapper can return a null key")
                .input("in").output("out", Format.STRING, Format.LONG).compareShape()
                .reference(builder -> builder.stream("in", CONSUMED)
                        .groupBy((k, v) -> user(v), GROUPED)
                        .count(Materialized.with(Serdes.String(), Serdes.Long()))
                        .toStream()
                        .to("out", PRODUCED_LONG))
                .ksml("parity/c13-group-by-count.yaml")
                .pipe("in", "k1", "{\"user\":\"alice\"}", 1000)
                .pipe("in", "k2", "{\"user\":\"bob\"}", 2000)
                .pipe("in", "k3", "{\"user\":\"alice\"}", 3000)
                .pipe("in", "k4", "{\"other\":1}", 4000)
                .check();
    }

    @Test
    void c14Reduce() {
        ParityCase.of("C14", "groupByKey + reduce: a null value is skipped")
                .input("in").output("out")
                .reference(builder -> builder.stream("in", CONSUMED)
                        .groupByKey(GROUPED)
                        .reduce((v1, v2) -> Json.str(Json.obj().put("amount", Json.num(v1, "amount") + Json.num(v2, "amount"))),
                                Materialized.with(Serdes.String(), Serdes.String()))
                        .toStream()
                        .to("out", PRODUCED))
                .ksml("parity/c14-reduce.yaml")
                .pipe("in", "a", "{\"amount\":1}", 1000)
                .pipe("in", "a", "{\"amount\":2}", 2000)
                .pipe("in", "a", null, 3000)
                .pipe("in", "a", "{\"amount\":4}", 4000)
                .knownDifference("F01", "a tombstone reaches the reducer as None instead of being skipped, the app stops")
                .check();
    }

    @Test
    void c15Aggregate() {
        ParityCase.of("C15", "groupByKey + aggregate: initializer once per key, a null value is skipped")
                .input("in").output("out").store("agg_store")
                .reference(builder -> builder.stream("in", CONSUMED)
                        .groupByKey(GROUPED)
                        .aggregate(() -> "{\"count\":0,\"sum\":0}", (k, v, agg) -> sumAndCount(agg, v),
                                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("agg_store")
                                        .withKeySerde(Serdes.String()).withValueSerde(Serdes.String()))
                        .toStream()
                        .to("out", PRODUCED))
                .ksml("parity/c15-aggregate.yaml")
                .pipe("in", "a", "{\"amount\":1}", 1000)
                .pipe("in", "b", "{\"amount\":10}", 2000)
                .pipe("in", "a", "{\"amount\":2}", 3000)
                .pipe("in", "a", null, 4000)
                .pipe("in", "a", "{\"amount\":4}", 5000)
                .knownDifference("F01", "a tombstone reaches the aggregator as None instead of being skipped, the app stops")
                .check();
    }

    @Test
    void c15bAggregateWithNoneGuard() {
        ParityCase.of("C15b", "groupByKey + aggregate where the KSML aggregator guards against None")
                .input("in").output("out").store("agg_store")
                .reference(builder -> builder.stream("in", CONSUMED)
                        .groupByKey(GROUPED)
                        .aggregate(() -> "{\"count\":0,\"sum\":0}", (k, v, agg) -> sumAndCount(agg, v),
                                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("agg_store")
                                        .withKeySerde(Serdes.String()).withValueSerde(Serdes.String()))
                        .toStream()
                        .to("out", PRODUCED))
                .ksml("parity/c15b-aggregate-guarded.yaml")
                .pipe("in", "a", "{\"amount\":1}", 1000)
                .pipe("in", "b", "{\"amount\":10}", 2000)
                .pipe("in", "a", "{\"amount\":2}", 3000)
                .pipe("in", "a", null, 4000)
                .pipe("in", "a", "{\"amount\":4}", 5000)
                .knownDifference("F01", "even with a None guard, KSML writes one extra aggregate record for the tombstone")
                .check();
    }

    @Test
    void c16CogroupSingleStream() {
        ParityCase.of("C16", "cogroup with one stream gives the same result as aggregate")
                .input("in").output("out")
                .reference(builder -> builder.stream("in", CONSUMED)
                        .groupByKey(GROUPED)
                        .<String>cogroup((k, v, agg) -> sumAndCount(agg, v))
                        .aggregate(() -> "{\"count\":0,\"sum\":0}", Materialized.with(Serdes.String(), Serdes.String()))
                        .toStream()
                        .to("out", PRODUCED))
                .ksml("parity/c16-cogroup-single.yaml")
                .pipe("in", "a", "{\"amount\":1}", 1000)
                .pipe("in", "b", "{\"amount\":10}", 2000)
                .pipe("in", "a", "{\"amount\":2}", 3000)
                .check();
    }

    @Test
    void c17CogroupTwoStreams() {
        ParityCase.of("C17", "cogroup with two streams into one aggregate")
                .input("in_a").input("in_b").output("out")
                .reference(builder -> {
                    final var a = builder.stream("in_a", CONSUMED).groupByKey(GROUPED);
                    final var b = builder.stream("in_b", CONSUMED).groupByKey(GROUPED);
                    a.<String>cogroup((k, v, agg) -> Json.str(Json.obj().put("a", Json.num(agg, "a") + Json.num(v, "n")).put("b", Json.num(agg, "b"))))
                            .cogroup(b, (k, v, agg) -> Json.str(Json.obj().put("a", Json.num(agg, "a")).put("b", Json.num(agg, "b") + Json.num(v, "n"))))
                            .aggregate(() -> "{\"a\":0,\"b\":0}", Materialized.with(Serdes.String(), Serdes.String()))
                            .toStream()
                            .to("out", PRODUCED);
                })
                .ksml("parity/c17-cogroup-two.yaml")
                .pipe("in_a", "k", "{\"n\":1}", 1000)
                .pipe("in_b", "k", "{\"n\":10}", 2000)
                .pipe("in_a", "k", "{\"n\":2}", 3000)
                .check();
    }
}
