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
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;

/**
 * Parity cases for stream-stream joins.
 */
@ExtendWith(KSMLTestExtension.class)
class StreamJoinParityTest {
    private static final Consumed<String, String> CONSUMED = Consumed.with(Serdes.String(), Serdes.String());
    private static final Produced<String, String> PRODUCED = Produced.with(Serdes.String(), Serdes.String());
    private static final StreamJoined<String, String, String> JOINED = StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String());
    private static final JoinWindows WINDOW = JoinWindows.ofTimeDifferenceAndGrace(Duration.ofSeconds(10), Duration.ZERO);

    static String joined(String key, String left, String right) {
        final var result = Json.obj();
        if (key == null) result.putNull("key");
        else result.put("key", key);
        if (left == null) result.putNull("left");
        else result.set("left", Json.obj(left).get("v"));
        if (right == null) result.putNull("right");
        else result.set("right", Json.obj(right).get("v"));
        return Json.str(result);
    }

    @Test
    void c30StreamStreamJoinAndLeftJoin() {
        ParityCase.of("C30", "stream-stream join and leftJoin: window edges, a late record, the left-only result comes when the window closes")
                .input("left").input("right").output("out_join").output("out_left")
                .reference(builder -> {
                    final var left = builder.stream("left", CONSUMED);
                    final var right = builder.stream("right", CONSUMED);
                    left.join(right, StreamJoinParityTest::joined, WINDOW, JOINED).to("out_join", PRODUCED);
                    left.leftJoin(right, StreamJoinParityTest::joined, WINDOW, JOINED).to("out_left", PRODUCED);
                })
                .ksml("parity/c30-stream-join-left-join.yaml")
                .pipe("left", "k1", "{\"v\":1}", 1000)
                .pipe("right", "k1", "{\"v\":2}", 5000)
                .pipe("right", "k1", "{\"v\":3}", 12000)
                .pipe("left", "k2", "{\"v\":4}", 3000)
                .pipe("left", "k1", "{\"v\":5}", 15000)
                .pipe("left", "k1", "{\"v\":6}", 2000)
                .pipe("left", "k9", "{\"v\":9}", 40000)
                .check();
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

    @Test
    void c33StreamTableJoin() {
        ParityCase.of("C33", "stream-table join and leftJoin: table updated and deleted, a null value and a null key on the stream")
                .input("s_in").input("t_tab").output("out_join").output("out_left")
                .reference(builder -> {
                    final var customers = builder.table("t_tab", CONSUMED);
                    final var orders = builder.stream("s_in", CONSUMED);
                    final var joined = Joined.with(Serdes.String(), Serdes.String(), Serdes.String());
                    orders.join(customers, StreamJoinParityTest::streamTableJoined, joined).to("out_join", PRODUCED);
                    orders.leftJoin(customers, StreamJoinParityTest::streamTableJoined, joined).to("out_left", PRODUCED);
                })
                .ksml("parity/c33-stream-table-join.yaml")
                .pipe("s_in", "k1", "{\"v\":1}", 1000)
                .pipe("t_tab", "k1", "{\"name\":\"Ann\"}", 2000)
                .pipe("s_in", "k1", "{\"v\":2}", 3000)
                .pipe("t_tab", "k1", null, 4000)
                .pipe("s_in", "k1", "{\"v\":3}", 5000)
                .pipe("s_in", "k2", null, 6000)
                .pipe("s_in", null, "{\"v\":7}", 7000)
                .knownDifference("F01", "a stream record with a null value is joined instead of dropped", 6)
                .check();
    }

    private static String globalJoined(String key, String order, String customer) {
        final var result = Json.obj();
        if (key == null) result.putNull("key");
        else result.put("key", key);
        if (order == null) result.putNull("amount");
        else result.put("amount", Json.num(order, "amount"));
        if (customer == null) result.putNull("name");
        else result.put("name", Json.text(customer, "name"));
        return Json.str(result);
    }

    @Test
    void c34StreamGlobalTableJoin() {
        ParityCase.of("C34", "stream-globalTable join and leftJoin: the key mapper returns null, a null value on the stream")
                .input("s_in").input("g_customers").global("g_customers").output("out_join").output("out_left")
                .reference(builder -> {
                    final var customers = builder.globalTable("g_customers", CONSUMED);
                    final var orders = builder.stream("s_in", CONSUMED);
                    orders.join(customers, (k, v) -> v == null ? null : Json.text(v, "customer"), StreamJoinParityTest::globalJoined).to("out_join", PRODUCED);
                    orders.leftJoin(customers, (k, v) -> v == null ? null : Json.text(v, "customer"), StreamJoinParityTest::globalJoined).to("out_left", PRODUCED);
                })
                .ksml("parity/c34-stream-global-table-join.yaml")
                .pipe("g_customers", "c1", "{\"name\":\"Ann\"}", 1000)
                .pipe("s_in", "o1", "{\"customer\":\"c1\",\"amount\":10}", 2000)
                .pipe("s_in", "o2", "{\"customer\":\"c2\",\"amount\":5}", 3000)
                .pipe("s_in", "o3", "{\"amount\":7}", 4000)
                .pipe("s_in", "o4", null, 5000)
                .knownDifference("F07", "a key mapper that returns None makes the global table join throw a NullPointerException", 6)
                .check();
    }

    @Test
    void c32OuterJoin() {
        ParityCase.of("C32", "stream-stream outerJoin, the joiner gets the key")
                .input("left").input("right").output("out")
                .reference(builder -> {
                    final var left = builder.stream("left", CONSUMED);
                    final var right = builder.stream("right", CONSUMED);
                    left.outerJoin(right, StreamJoinParityTest::joined, WINDOW, JOINED).to("out", PRODUCED);
                })
                .ksml("parity/c32-stream-outer-join.yaml")
                .pipe("left", "k1", "{\"v\":1}", 1000)
                .pipe("right", "k1", "{\"v\":2}", 1500)
                .pipe("left", "k2", "{\"v\":3}", 2000)
                .pipe("right", "k3", "{\"v\":4}", 3000)
                .pipe("left", "k9", "{\"v\":9}", 30000)
                .knownDifference("F08", "the stream-stream outerJoin passes key None to the joiner", 6)
                .check();
    }

    @Test
    void c42OuterJoinWithLoggingEnabled() {
        ParityCase.of("C42", "stream-stream outerJoin with logging: true on both KSML stores: which changelog topics are written")
                .input("left").input("right").output("out").compareChangelogs()
                .reference(builder -> {
                    final var left = builder.stream("left", CONSUMED);
                    final var right = builder.stream("right", CONSUMED);
                    left.outerJoin(right, StreamJoinParityTest::joined, WINDOW, JOINED).to("out", PRODUCED);
                })
                .ksml("parity/c42-outer-join-logging.yaml")
                .pipe("left", "k1", "{\"v\":1}", 1000)
                .pipe("right", "k1", "{\"v\":2}", 1500)
                .pipe("left", "k2", "{\"v\":3}", 2000)
                .pipe("left", "k9", "{\"v\":9}", 30000)
                .knownDifference("F08", "the stream-stream outerJoin passes key None to the joiner", 4)
                .check();
    }
}
