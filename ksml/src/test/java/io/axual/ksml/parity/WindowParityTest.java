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
import org.apache.kafka.streams.kstream.EmitStrategy;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;

/**
 * Parity cases for windowed aggregations and suppression. Windowed keys are written as
 * {@code key@start/end} on both sides, so the window boundaries are part of the comparison.
 */
@ExtendWith(KSMLTestExtension.class)
class WindowParityTest {
    private static final Consumed<String, String> CONSUMED = Consumed.with(Serdes.String(), Serdes.String());
    private static final Produced<String, Long> PRODUCED_LONG = Produced.with(Serdes.String(), Serdes.Long());
    private static final Grouped<String, String> GROUPED = Grouped.with(Serdes.String(), Serdes.String());

    private static String windowKey(Windowed<String> key, Long value) {
        return key.key() + "@" + key.window().start() + "/" + key.window().end();
    }

    @Test
    void c24TumblingCount() {
        ParityCase.of("C24", "tumbling window count: record on the window end, out-of-order inside grace, late record after grace")
                .input("in").output("out", Format.STRING, Format.LONG)
                .reference(builder -> builder.stream("in", CONSUMED)
                        .groupByKey(GROUPED)
                        .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofSeconds(10), Duration.ofSeconds(5)))
                        .count(Materialized.with(Serdes.String(), Serdes.Long()))
                        .toStream(WindowParityTest::windowKey)
                        .to("out", PRODUCED_LONG))
                .ksml("parity/c24-tumbling-count.yaml")
                .pipe("in", "a", "{\"n\":1}", 1000)
                .pipe("in", "a", "{\"n\":2}", 9999)
                .pipe("in", "a", "{\"n\":3}", 10000)
                .pipe("in", "b", "{\"n\":4}", 12000)
                .pipe("in", "a", "{\"n\":5}", 4000)
                .pipe("in", "x", "{\"n\":6}", 16000)
                .pipe("in", "a", "{\"n\":7}", 5000)
                .pipe("in", "a", "{\"n\":8}", 11000)
                .check();
    }

    @Test
    void c24bTumblingCountWithToStreamMapper() {
        ParityCase.of("C24b", "windowed count, the windowed key is mapped in toStream")
                .input("in").output("out", Format.STRING, Format.LONG)
                .reference(builder -> builder.stream("in", CONSUMED)
                        .groupByKey(GROUPED)
                        .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofSeconds(10), Duration.ofSeconds(5)))
                        .count(Materialized.with(Serdes.String(), Serdes.Long()))
                        .toStream(WindowParityTest::windowKey)
                        .to("out", PRODUCED_LONG))
                .ksml("parity/c24b-tumbling-count-tostream-mapper.yaml")
                .pipe("in", "a", "{\"n\":1}", 1000)
                .pipe("in", "b", "{\"n\":2}", 2000)
                .knownDifference("F09", "toStream with a mapper on a windowed table fails: the key is a struct, not a Window", 8)
                .check();
    }

    @Test
    void c25HoppingAggregate() {
        ParityCase.of("C25", "hopping window aggregate: one record lands in two windows")
                .input("in").output("out", Format.STRING, Format.LONG)
                .reference(builder -> builder.stream("in", CONSUMED)
                        .groupByKey(GROUPED)
                        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(10)).advanceBy(Duration.ofSeconds(5)))
                        .aggregate(() -> 0L, (k, v, agg) -> agg + Json.num(v, "amount"), Materialized.with(Serdes.String(), Serdes.Long()))
                        .toStream(WindowParityTest::windowKey)
                        .to("out", PRODUCED_LONG))
                .ksml("parity/c25-hopping-aggregate.yaml")
                .pipe("in", "a", "{\"amount\":1}", 1000)
                .pipe("in", "a", "{\"amount\":2}", 6000)
                .pipe("in", "a", "{\"amount\":4}", 12000)
                .check();
    }

    @Test
    void c26SlidingCount() {
        ParityCase.of("C26", "sliding window count")
                .input("in").output("out", Format.STRING, Format.LONG)
                .reference(builder -> builder.stream("in", CONSUMED)
                        .groupByKey(GROUPED)
                        .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(Duration.ofSeconds(5), Duration.ZERO))
                        .count(Materialized.with(Serdes.String(), Serdes.Long()))
                        .toStream(WindowParityTest::windowKey)
                        .to("out", PRODUCED_LONG))
                .ksml("parity/c26-sliding-count.yaml")
                .pipe("in", "a", "{\"n\":1}", 1000)
                .pipe("in", "a", "{\"n\":2}", 3000)
                .pipe("in", "a", "{\"n\":3}", 7000)
                .pipe("in", "a", "{\"n\":4}", 20000)
                .check();
    }

    @Test
    void c27SessionAggregateWithMerger() {
        ParityCase.of("C27", "session window aggregate: a late record merges two sessions")
                .input("in").output("out", Format.STRING, Format.LONG)
                .reference(builder -> builder.stream("in", CONSUMED)
                        .groupByKey(GROUPED)
                        .windowedBy(SessionWindows.ofInactivityGapAndGrace(Duration.ofSeconds(5), Duration.ZERO))
                        .aggregate(() -> 0L, (k, v, agg) -> agg + Json.num(v, "amount"), (k, a1, a2) -> a1 + a2,
                                Materialized.with(Serdes.String(), Serdes.Long()))
                        .toStream(WindowParityTest::windowKey)
                        .to("out", PRODUCED_LONG))
                .ksml("parity/c27-session-aggregate.yaml")
                .pipe("in", "a", "{\"amount\":1}", 1000)
                .pipe("in", "a", "{\"amount\":2}", 10000)
                .pipe("in", "a", "{\"amount\":4}", 5500)
                .pipe("in", "a", "{\"amount\":8}", 30000)
                .knownDifference("F14", "the required session store retention (1h here) changes which sessions merge", 10)
                .check();
    }

    @Test
    void c27bSessionAggregateWithMatchingRetention() {
        ParityCase.of("C27b", "session window aggregate, KSML store retention set to gap + grace (the Kafka Streams default)")
                .input("in").output("out", Format.STRING, Format.LONG)
                .reference(builder -> builder.stream("in", CONSUMED)
                        .groupByKey(GROUPED)
                        .windowedBy(SessionWindows.ofInactivityGapAndGrace(Duration.ofSeconds(5), Duration.ZERO))
                        .aggregate(() -> 0L, (k, v, agg) -> agg + Json.num(v, "amount"), (k, a1, a2) -> a1 + a2,
                                Materialized.with(Serdes.String(), Serdes.Long()))
                        .toStream(WindowParityTest::windowKey)
                        .to("out", PRODUCED_LONG))
                .ksml("parity/c27b-session-aggregate-retention.yaml")
                .pipe("in", "a", "{\"amount\":1}", 1000)
                .pipe("in", "a", "{\"amount\":2}", 10000)
                .pipe("in", "a", "{\"amount\":4}", 5500)
                .pipe("in", "a", "{\"amount\":8}", 30000)
                .check();
    }

    @Test
    void c23SuppressUntilWindowCloses() {
        ParityCase.of("C23", "windowed count + suppress untilWindowCloses: one final result per window")
                .input("in").output("out", Format.STRING, Format.LONG)
                .reference(builder -> builder.stream("in", CONSUMED)
                        .groupByKey(GROUPED)
                        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(10)))
                        .count(Materialized.with(Serdes.String(), Serdes.Long()))
                        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                        .toStream(WindowParityTest::windowKey)
                        .to("out", PRODUCED_LONG))
                .ksml("parity/c23-suppress-window-closes.yaml")
                .pipe("in", "a", "{\"n\":1}", 1000)
                .pipe("in", "a", "{\"n\":2}", 2000)
                .pipe("in", "b", "{\"n\":3}", 3000)
                .pipe("in", "a", "{\"n\":4}", 12000)
                .pipe("in", "x", "{\"n\":5}", 25000)
                .check();
    }

    @Test
    void c28EmitStrategyOnWindowCloseVersusSuppress() {
        ParityCase.of("C28", "Kafka Streams emitStrategy(onWindowClose) against the KSML workaround with suppress")
                .input("in").output("out", Format.STRING, Format.LONG)
                // Kafka Streams checks for closed windows at most once per second of wall-clock time, so on real Kafka
                // it would emit nothing in the short test
                .config("__emit.interval.ms.kstreams.windowed.aggregation__", "0")
                .reference(builder -> builder.stream("in", CONSUMED)
                        .groupByKey(GROUPED)
                        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(10)))
                        .emitStrategy(EmitStrategy.onWindowClose())
                        .count(Materialized.with(Serdes.String(), Serdes.Long()))
                        .toStream(WindowParityTest::windowKey)
                        .to("out", PRODUCED_LONG))
                .ksml("parity/c23-suppress-window-closes.yaml")
                .pipe("in", "a", "{\"n\":1}", 1000)
                .pipe("in", "a", "{\"n\":2}", 2000)
                .pipe("in", "b", "{\"n\":3}", 3000)
                .advanceWallClock(2000)
                .pipe("in", "a", "{\"n\":4}", 12000)
                .advanceWallClock(2000)
                .pipe("in", "x", "{\"n\":5}", 25000)
                .check();
    }

    @Test
    void c29WindowedCogroup() {
        ParityCase.of("C29", "cogroup, then a tumbling window, then aggregate")
                .input("in_a").output("out", Format.STRING, Format.LONG)
                .reference(builder -> builder.stream("in_a", CONSUMED)
                        .groupByKey(GROUPED)
                        .<Long>cogroup((k, v, agg) -> agg + Json.num(v, "n"))
                        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(10)))
                        .aggregate(() -> 0L, Materialized.with(Serdes.String(), Serdes.Long()))
                        .toStream(WindowParityTest::windowKey)
                        .to("out", PRODUCED_LONG))
                .ksml("parity/c29-windowed-cogroup.yaml")
                .pipe("in_a", "k", "{\"n\":1}", 1000)
                .pipe("in_a", "k", "{\"n\":2}", 5000)
                .pipe("in_a", "k", "{\"n\":4}", 15000)
                .check();
    }
}
