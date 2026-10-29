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
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Parity cases for the topology as a whole: repartition topics, optimization, changelog topics and errors.
 */
@ExtendWith(KSMLTestExtension.class)
class TopologyParityTest {
    private static final Consumed<String, String> CONSUMED = Consumed.with(Serdes.String(), Serdes.String());
    private static final Produced<String, String> PRODUCED = Produced.with(Serdes.String(), Serdes.String());
    private static final Grouped<String, String> GROUPED = Grouped.with(Serdes.String(), Serdes.String());

    @Test
    void c09bRepartition() {
        ParityCase.of("C09b", "repartition with numberOfPartitions")
                .input("in").output("out").compareShape()
                // Each repartition partition has its own task, and the tasks run in any order
                .anyOrderInDocker()
                .reference(builder -> builder.stream("in", CONSUMED)
                        .repartition(Repartitioned.with(Serdes.String(), Serdes.String()).withNumberOfPartitions(3))
                        .to("out", PRODUCED))
                .ksml("parity/c09b-repartition.yaml")
                .pipe("in", "k1", "{\"n\":1}", 1000)
                .pipe("in", "k2", "{\"n\":2}", 2000)
                .check();
    }

    @Test
    void c41OptimizationMergesRepartitionTopics() {
        ParityCase.of("C41", "two aggregations on one re-keyed stream: optimization 'all' merges the repartition topics")
                .input("in").output("out_count", Format.STRING, Format.LONG).output("out_reduce").compareShape()
                .reference(builder -> {
                    final var rekeyed = builder.stream("in", CONSUMED).selectKey((k, v) -> Json.text(v, "user"));
                    rekeyed.groupByKey(GROUPED).count(Materialized.with(Serdes.String(), Serdes.Long()))
                            .toStream().to("out_count", Produced.with(Serdes.String(), Serdes.Long()));
                    rekeyed.groupByKey(GROUPED)
                            .reduce((v1, v2) -> Json.str(Json.obj().put("amount", Json.num(v1, "amount") + Json.num(v2, "amount"))),
                                    Materialized.with(Serdes.String(), Serdes.String()))
                            .toStream().to("out_reduce", PRODUCED);
                })
                .ksml("parity/c41-optimization-repartition.yaml")
                .pipe("in", "k1", "{\"user\":\"alice\",\"amount\":1}", 1000)
                .pipe("in", "k2", "{\"user\":\"bob\",\"amount\":2}", 2000)
                .pipe("in", "k3", "{\"user\":\"alice\",\"amount\":3}", 3000)
                .knownDifference("F12", "optimization 'all' does not merge the repartition topics of a KSML selectKey", 1)
                .check();
    }

    @Test
    void c42bAggregateStoreWithKafkaStreamsDefaults() {
        ParityCase.of("C42b", "aggregate with a KSML store set to persistent, logging and timestamped: same changelog as Kafka Streams")
                .input("in").output("out").store("agg_store").compareChangelogs()
                .reference(builder -> builder.stream("in", CONSUMED)
                        .groupByKey(GROUPED)
                        .aggregate(() -> "{\"count\":0,\"sum\":0}", (k, v, agg) -> Json.str(Json.obj()
                                        .put("count", Json.num(agg, "count") + 1).put("sum", Json.num(agg, "sum") + Json.num(v, "amount"))),
                                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("agg_store")
                                        .withKeySerde(Serdes.String()).withValueSerde(Serdes.String()))
                        .toStream().to("out", PRODUCED))
                .ksml("parity/c42b-aggregate-store-like-kafka-streams.yaml")
                .pipe("in", "a", "{\"amount\":1}", 1000)
                .pipe("in", "a", "{\"amount\":2}", 2000)
                .check();
    }

    @Test
    void c43ErrorInUserFunction() {
        ParityCase.of("C43", "an exception in a user function stops processing at the same record")
                .input("in").output("out")
                .reference(builder -> builder.stream("in", CONSUMED)
                        .mapValues(v -> Json.str(Json.obj().put("ratio", Math.floorDiv(Json.num(v, "amount"), Json.num(v, "div")))))
                        .to("out", PRODUCED))
                .ksml("parity/c43-error-in-function.yaml")
                .pipe("in", "k1", "{\"amount\":10,\"div\":2}", 1000)
                .pipe("in", "k2", "{\"amount\":10,\"div\":0}", 2000)
                .pipe("in", "k3", "{\"amount\":9,\"div\":3}", 3000)
                .check();
    }
}
