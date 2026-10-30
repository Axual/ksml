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
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Map;

/**
 * Parity cases for the stateless KStream operations.
 */
@ExtendWith(KSMLTestExtension.class)
class KStreamStatelessParityTest {
    private static final Consumed<String, String> CONSUMED = Consumed.with(Serdes.String(), Serdes.String());
    private static final Produced<String, String> PRODUCED = Produced.with(Serdes.String(), Serdes.String());

    private static boolean isBig(String value) {
        return value != null && Json.num(value, "amount") > 10;
    }

    @Test
    void c01FilterAndFilterNot() {
        ParityCase.of("C01", "filter / filterNot, with a null key and a null value")
                .input("in").output("out_filter").output("out_filternot")
                .reference(builder -> {
                    final var input = builder.stream("in", CONSUMED);
                    input.filter((k, v) -> isBig(v)).to("out_filter", PRODUCED);
                    input.filterNot((k, v) -> isBig(v)).to("out_filternot", PRODUCED);
                })
                .ksml("parity/c01-filter.yaml")
                .pipe("in", "k1", "{\"amount\":5}", 1000)
                .pipe("in", "k2", "{\"amount\":20}", 2000)
                .pipe("in", null, "{\"amount\":30}", 3000)
                .pipe("in", "k3", null, 4000)
                .pipe("in", "k4", "{\"amount\":11}", 5000)
                .check();
    }

    @Test
    void c02MapValuesKeepsTimestampAndHeaders() {
        ParityCase.of("C02", "mapValues: null key, null value, None result, timestamp and headers are kept")
                .input("in").output("out").compareHeaders()
                .reference(builder -> builder.stream("in", CONSUMED)
                        .mapValues((k, v) -> {
                            if (v == null) return null;
                            final var result = Json.obj();
                            if (k == null) result.putNull("key");
                            else result.put("key", k);
                            result.put("double", Json.num(v, "amount") * 2);
                            return Json.str(result);
                        })
                        .to("out", PRODUCED))
                .ksml("parity/c02-map-values.yaml")
                .pipe("in", "k1", "{\"amount\":5}", 1000, Map.of("trace", "t-1"))
                .pipe("in", null, "{\"amount\":7}", 2000)
                .pipe("in", "k3", null, 3000, Map.of("trace", "t-3"))
                .pipe("in", "k4", "{\"amount\":-1}", 500)
                .check();
    }

    @Test
    void c04Map() {
        ParityCase.of("C04", "map (transformKeyValue): the new key can be null")
                .input("in").output("out")
                .reference(builder -> builder.stream("in", CONSUMED)
                        .map((k, v) -> {
                            final var newKey = v == null ? null : Json.text(v, "user");
                            final var value = Json.obj();
                            if (k == null) value.putNull("orig");
                            else value.put("orig", k);
                            return KeyValue.pair(newKey, Json.str(value));
                        })
                        .to("out", PRODUCED))
                .ksml("parity/c04-map.yaml")
                .pipe("in", "k1", "{\"user\":\"alice\"}", 1000)
                .pipe("in", "k2", "{\"other\":1}", 2000)
                .pipe("in", "k3", null, 3000)
                .check();
    }

    @Test
    void c05FlatMapAndFlatMapValues() {
        ParityCase.of("C05", "flatMap / flatMapValues: empty list and a null item")
                .input("in").output("out_flatmap").output("out_flatmapvalues")
                .reference(builder -> {
                    final var input = builder.stream("in", CONSUMED);
                    input.flatMap((k, v) -> {
                        final var result = new ArrayList<KeyValue<String, String>>();
                        final var items = Json.obj(v).get("items");
                        for (var i = 0; i < items.size(); i++) {
                            result.add(KeyValue.pair(k + "-" + i, Json.str(Json.obj().set("item", items.get(i)))));
                        }
                        return result;
                    }).to("out_flatmap", PRODUCED);
                    input.flatMapValues(v -> {
                        final var result = new ArrayList<String>();
                        Json.obj(v).get("items").forEach(item -> result.add(Json.str(Json.obj().set("item", item))));
                        return result;
                    }).to("out_flatmapvalues", PRODUCED);
                })
                .ksml("parity/c05-flat-map.yaml")
                .pipe("in", "k1", "{\"items\":[1,2,3]}", 1000)
                .pipe("in", "k2", "{\"items\":[]}", 2000)
                .pipe("in", "k3", "{\"items\":[4,null]}", 3000)
                .check();
    }

    private static String call(String key, String value) {
        return (key == null ? "null" : key) + "=" + (value == null ? "null" : String.valueOf(Json.num(value, "amount")));
    }

    @Test
    void c06PeekAndForEach() {
        ParityCase.of("C06", "peek passes every record on, forEach is called once per record, in order")
                .input("in").output("out").store("peek_calls").store("foreach_calls")
                .reference(builder -> {
                    builder.addStateStore(Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore("peek_calls"), Serdes.String(), Serdes.String()).withLoggingDisabled());
                    builder.addStateStore(Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore("foreach_calls"), Serdes.String(), Serdes.String()).withLoggingDisabled());
                    final var input = builder.stream("in", CONSUMED);
                    input.processValues(() -> new FixedKeyProcessor<String, String, String>() {
                        private FixedKeyProcessorContext<String, String> context;
                        private KeyValueStore<String, String> store;
                        private int counter;

                        @Override
                        public void init(FixedKeyProcessorContext<String, String> context) {
                            this.context = context;
                            this.store = context.getStateStore("peek_calls");
                        }

                        @Override
                        public void process(FixedKeyRecord<String, String> rec) {
                            store.put(String.format("%03d", ++counter), call(rec.key(), rec.value()));
                            context.forward(rec);
                        }
                    }, "peek_calls").to("out", PRODUCED);
                    input.process(() -> new Processor<String, String, Void, Void>() {
                        private KeyValueStore<String, String> store;
                        private int counter;

                        @Override
                        public void init(ProcessorContext<Void, Void> context) {
                            this.store = context.getStateStore("foreach_calls");
                        }

                        @Override
                        public void process(Record<String, String> rec) {
                            store.put(String.format("%03d", ++counter), call(rec.key(), rec.value()));
                        }
                    }, "foreach_calls");
                })
                .ksml("parity/c06-peek-foreach.yaml")
                .pipe("in", "k1", "{\"amount\":1}", 1000)
                .pipe("in", null, "{\"amount\":2}", 2000)
                .pipe("in", "k3", null, 3000)
                .pipe("in", "k4", "{\"amount\":4}", 4000)
                .check();
    }

    @Test
    void c07Merge() {
        ParityCase.of("C07", "merge keeps the order and the timestamps of both inputs")
                .input("in_a").input("in_b").output("out")
                .reference(builder -> builder.stream("in_a", CONSUMED).merge(builder.stream("in_b", CONSUMED)).to("out", PRODUCED))
                .ksml("parity/c07-merge.yaml")
                .pipe("in_a", "a1", "{\"n\":1}", 1000)
                .pipe("in_b", "b1", "{\"n\":2}", 500)
                .pipe("in_a", "a2", "{\"n\":3}", 3000)
                .pipe("in_b", null, "{\"n\":4}", 2000)
                .check();
    }

    private static boolean amountAbove(String value, int limit) {
        return value != null && Json.num(value, "amount") > limit;
    }

    @Test
    void c08Branch() {
        ParityCase.of("C08", "split/branch: first match wins, default branch, no default branch")
                .input("in").output("out_big").output("out_medium").output("out_default").output("nd_big").output("nd_medium")
                .reference(builder -> {
                    final var input = builder.stream("in", CONSUMED);
                    input.split()
                            .branch((k, v) -> amountAbove(v, 100), Branched.withConsumer(s -> s.to("out_big", PRODUCED)))
                            .branch((k, v) -> amountAbove(v, 10), Branched.withConsumer(s -> s.to("out_medium", PRODUCED)))
                            .defaultBranch(Branched.withConsumer(s -> s.to("out_default", PRODUCED)));
                    input.split()
                            .branch((k, v) -> amountAbove(v, 100), Branched.withConsumer(s -> s.to("nd_big", PRODUCED)))
                            .branch((k, v) -> amountAbove(v, 10), Branched.withConsumer(s -> s.to("nd_medium", PRODUCED)))
                            .noDefaultBranch();
                })
                .ksml("parity/c08-branch.yaml")
                .pipe("in", "k1", "{\"amount\":500}", 1000)
                .pipe("in", "k2", "{\"amount\":50}", 2000)
                .pipe("in", "k3", "{\"amount\":5}", 3000)
                .pipe("in", "k4", null, 4000)
                .check();
    }

    @Test
    void c09TopicNameExtractor() {
        ParityCase.of("C09", "to(TopicNameExtractor) routes each record to a topic named in the record")
                .input("in").output("route_eu").output("route_us")
                .reference(builder -> builder.stream("in", CONSUMED)
                        .to((k, v, ctx) -> "route_" + Json.text(v, "region"), PRODUCED))
                .ksml("parity/c09-topic-name-extractor.yaml")
                .pipe("in", "k1", "{\"region\":\"eu\"}", 1000)
                .pipe("in", "k2", "{\"region\":\"us\"}", 2000)
                .pipe("in", "k3", "{\"region\":\"eu\"}", 3000)
                .check();
    }

    @Test
    void c10ToTable() {
        ParityCase.of("C10", "toTable: updates, a tombstone deletes the key")
                .input("in").output("out").store("table_store")
                .reference(builder -> builder.stream("in", CONSUMED)
                        .toTable(Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("table_store")
                                .withKeySerde(Serdes.String()).withValueSerde(Serdes.String()))
                        .toStream()
                        .to("out", PRODUCED))
                .ksml("parity/c10-to-table.yaml")
                .pipe("in", "k1", "{\"v\":1}", 1000)
                .pipe("in", "k1", "{\"v\":2}", 2000)
                .pipe("in", "k2", "{\"v\":3}", 3000)
                .pipe("in", "k1", null, 4000)
                .check();
    }

    @Test
    void c11Print() {
        ParityCase.of("C11", "print to a file with a label")
                .input("in")
                .printFiles("target/parity-print-ref.txt", "target/parity-print-ksml.txt")
                .skipInDocker("print writes to a local file, which is checked with the test driver only")
                .reference(builder -> builder.stream("in", CONSUMED)
                        .print(Printed.<String, String>toFile("target/parity-print-ref.txt").withLabel("parity")))
                .ksml("parity/c11-print.yaml")
                .pipe("in", "k1", "{\"amount\":1}", 1000)
                .pipe("in", null, "{\"amount\":2}", 2000)
                .pipe("in", "k3", null, 3000)
                .check();
    }
}
