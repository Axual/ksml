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
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Optional;
import java.util.Set;

/**
 * Parity cases that only make sense on real Kafka: restarts, several partitions and partitioners.
 */
@ExtendWith(KSMLTestExtension.class)
class DockerOnlyParityTest {
    private static final Consumed<String, String> CONSUMED = Consumed.with(Serdes.String(), Serdes.String());
    private static final Produced<String, String> PRODUCED = Produced.with(Serdes.String(), Serdes.String());
    private static final Produced<String, Long> PRODUCED_LONG = Produced.with(Serdes.String(), Serdes.Long());
    private static final Grouped<String, String> GROUPED = Grouped.with(Serdes.String(), Serdes.String());

    private static void countReference(StreamsBuilder builder) {
        builder.stream("in", CONSUMED)
                .groupByKey(GROUPED)
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("count_store")
                        .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()))
                .toStream()
                .to("out", PRODUCED_LONG);
    }

    private static ParityCase countAcrossRestart(String id, String title, String ksml) {
        return ParityCase.of(id, title)
                .input("in").output("out", Format.STRING, Format.LONG)
                .dockerOnly()
                .reference(DockerOnlyParityTest::countReference)
                .ksml(ksml)
                .pipe("in", "a", "{\"n\":1}", 1000)
                .pipe("in", "b", "{\"n\":2}", 2000)
                .pipe("in", "a", "{\"n\":3}", 3000)
                .restartAfter(3)
                .pipe("in", "a", "{\"n\":4}", 4000)
                .pipe("in", "b", "{\"n\":5}", 5000);
    }

    @Test
    void c44aCountAcrossRestartWithDefaultStore() {
        countAcrossRestart("C44a", "count survives a restart: KSML store with default options", "parity/c12-count.yaml")
                .knownDifference("F06", "a store with default options is in memory and has no changelog, so the count starts again at 0")
                .run();
    }

    @Test
    void c44bCountAcrossRestartWithKafkaStreamsLikeStore() {
        countAcrossRestart("C44b", "count survives a restart: KSML store with persistent, logging and timestamped",
                "parity/c44b-count-store-like-kafka-streams.yaml").run();
    }

    @Test
    void c44cCountAcrossRestartWithoutStore() {
        countAcrossRestart("C44c", "count survives a restart: no KSML store definition", "parity/c44c-count-no-store.yaml").run();
    }

    @Test
    void c44dStreamTableJoinAcrossRestart() {
        ParityCase.of("C44d", "stream-table join still finds the table rows after a restart")
                .input("s_in").input("t_tab").output("out_join").output("out_left")
                .dockerOnly()
                .reference(builder -> {
                    final var customers = builder.table("t_tab", CONSUMED);
                    final var orders = builder.stream("s_in", CONSUMED);
                    final var joined = Joined.with(Serdes.String(), Serdes.String(), Serdes.String());
                    orders.join(customers, DockerOnlyParityTest::streamTableJoined, joined).to("out_join", PRODUCED);
                    orders.leftJoin(customers, DockerOnlyParityTest::streamTableJoined, joined).to("out_left", PRODUCED);
                })
                .ksml("parity/c33-stream-table-join.yaml")
                .pipe("t_tab", "k1", "{\"name\":\"Ann\"}", 1000)
                .pipe("s_in", "k1", "{\"v\":1}", 2000)
                .restartAfter(2)
                .pipe("s_in", "k1", "{\"v\":2}", 3000)
                .run();
    }

    private static String streamTableJoined(String key, String order, String customer) {
        final var result = Json.obj();
        if (key == null) result.putNull("key");
        else result.put("key", key);
        if (order == null) result.putNull("order");
        else result.set("order", Json.obj(order).get("v"));
        if (customer == null) result.putNull("customer");
        else result.put("customer", Json.text(customer, "name"));
        return Json.str(result);
    }

    private static final StreamPartitioner<String, String> BY_REGION = (topic, key, value, partitions) -> {
        final var region = Json.text(value, "region");
        return Optional.of(Set.of("eu".equals(region) ? 0 : "us".equals(region) ? 1 : 2));
    };

    @Test
    void c45ToWithPartitioner() {
        ParityCase.of("C45", "to with a partitioner on a topic with 3 partitions")
                .input("in").output("out").partitions("out", 3)
                .dockerOnly()
                .reference(builder -> builder.stream("in", CONSUMED)
                        .to("out", Produced.with(Serdes.String(), Serdes.String()).withStreamPartitioner(BY_REGION)))
                .ksml("parity/c45-to-partitioner.yaml")
                .pipe("in", "k1", "{\"region\":\"eu\"}", 1000)
                .pipe("in", "k2", "{\"region\":\"us\"}", 2000)
                .pipe("in", "k3", "{\"region\":\"apac\"}", 3000)
                .pipe("in", "k4", "{\"region\":\"eu\"}", 4000)
                .run();
    }

    @Test
    void c46RepartitionWithPartitioner() {
        ParityCase.of("C46", "repartition with numberOfPartitions and a partitioner, output on 3 partitions")
                .input("in").output("out").partitions("out", 3)
                .dockerOnly()
                .reference(builder -> builder.stream("in", CONSUMED)
                        .repartition(Repartitioned.with(Serdes.String(), Serdes.String()).withNumberOfPartitions(3).withStreamPartitioner(BY_REGION))
                        .peek((k, v) -> System.out.println("after repartition " + k + " " + v))
                        .to("out", PRODUCED))
                .ksml("parity/c46-repartition-partitioner.yaml")
                .pipe("in", "k1", "{\"region\":\"eu\"}", 1000)
                .pipe("in", "k2", "{\"region\":\"us\"}", 2000)
                .pipe("in", "k3", "{\"region\":\"apac\"}", 3000)
                .pipe("in", "k4", "{\"region\":\"eu\"}", 4000)
                .run();
    }

    @Test
    void c47GroupByAcrossPartitions() {
        ParityCase.of("C47", "groupBy + count with 3 input and 3 output partitions (repartition between tasks)")
                .input("in").output("out", Format.STRING, Format.LONG).partitions("in", 3).partitions("out", 3).compareFinalValues()
                .dockerOnly()
                .reference(builder -> builder.stream("in", CONSUMED)
                        .groupBy((k, v) -> Json.text(v, "user"), GROUPED)
                        .count(Materialized.with(Serdes.String(), Serdes.Long()))
                        .toStream()
                        .to("out", PRODUCED_LONG))
                .ksml("parity/c13-group-by-count.yaml")
                .pipe("in", "k1", "{\"user\":\"alice\"}", 1000)
                .pipe("in", "k2", "{\"user\":\"bob\"}", 2000)
                .pipe("in", "k3", "{\"user\":\"alice\"}", 3000)
                .pipe("in", "k4", "{\"user\":\"carol\"}", 4000)
                .pipe("in", "k5", "{\"user\":\"bob\"}", 5000)
                .pipe("in", "k6", "{\"user\":\"alice\"}", 6000)
                .run();
    }
}
