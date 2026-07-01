package io.axual.ksml.runner.producer;

/*-
 * ========================LICENSE_START=================================
 * KSML Runner
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

import io.axual.ksml.definition.ProducerDefinition;
import io.axual.ksml.metric.MetricTags;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ProducerStrategy}.
 * <p>
 * These tests deliberately use {@link ProducerDefinition}s without {@code condition} or {@code until}
 * functions, so no Python/GraalVM context is required: the strategy resolves those optional predicates
 * to {@code null}. The predicate-driven paths (custom condition/until functions) are exercised end-to-end
 * by {@code KafkaProducerRunnerTest} on GraalVM.
 */
class ProducerStrategyTest {

    private static final MetricTags TAGS = new MetricTags();

    private static ProducerStrategy strategyFor(Long messageCount, Long batchSize, Duration interval) {
        // generator, condition, until and target are unused by ProducerStrategy and left null on purpose.
        final var definition = new ProducerDefinition(null, null, null, null, messageCount, batchSize, interval);
        // context may be null because no condition/until functions are defined.
        return new ProducerStrategy(null, "ns", "producer", TAGS, definition);
    }

    @Test
    @DisplayName("With no interval, messageCount or until, the producer runs exactly once")
    void runsOnceWhenNothingConfigured() {
        final var strategy = strategyFor(null, null, null);

        assertThat(strategy.interval()).isEqualTo(Duration.ZERO);
        assertThat(strategy.batchSize()).isEqualTo(1);
        // "once" producers must never reschedule, even before anything is produced.
        assertThat(strategy.shouldReschedule()).isFalse();
        // Without a condition every message is valid, and without an "until" we always continue.
        assertThat(strategy.validateMessage(null, null)).isTrue();
        assertThat(strategy.continueAfterMessage(null, null)).isTrue();
    }

    @Test
    @DisplayName("A fixed messageCount reschedules until that many messages have been produced")
    void reschedulesUntilMessageCountReached() {
        final var strategy = strategyFor(3L, null, null);

        assertThat(strategy.shouldReschedule()).isTrue();
        assertThat(strategy.batchSize()).isEqualTo(1); // min(default batch 1, remaining 3)

        strategy.successfullyProducedOneMessage();
        strategy.successfullyProducedOneMessage();
        assertThat(strategy.messagesProduced()).isEqualTo(2);
        assertThat(strategy.shouldReschedule()).isTrue();

        strategy.successfullyProducedOneMessage();
        assertThat(strategy.messagesProduced()).isEqualTo(3);
        // All requested messages produced, so no further runs.
        assertThat(strategy.shouldReschedule()).isFalse();
    }

    @Test
    @DisplayName("An interval without a messageCount produces indefinitely")
    void reschedulesForeverWhenOnlyIntervalConfigured() {
        final var strategy = strategyFor(null, null, Duration.ofMillis(250));

        assertThat(strategy.interval()).isEqualTo(Duration.ofMillis(250));
        assertThat(strategy.shouldReschedule()).isTrue();

        // Producing many messages must not exhaust an infinite producer.
        for (int i = 0; i < 100; i++) {
            strategy.successfullyProducedOneMessage();
        }
        assertThat(strategy.shouldReschedule()).isTrue();
    }

    @ParameterizedTest(name = "configured batchSize {0} -> effective {1}")
    @DisplayName("Batch size is clamped to the supported 1..1000 range")
    @CsvSource({
            "0,    1",     // below the minimum falls back to the default of 1
            "1,    1",
            "500,  500",
            "1000, 1000",
            "2000, 1"      // above the maximum falls back to the default of 1
    })
    void clampsBatchSizeToValidRange(long configured, long expected) {
        // An interval (and no messageCount) makes the producer infinite, so batchSize() returns
        // the stored batch size verbatim without the "remaining messages" clamping.
        final var strategy = strategyFor(null, configured, Duration.ofMillis(10));
        assertThat(strategy.batchSize()).isEqualTo(expected);
    }

    @Test
    @DisplayName("Batch size never exceeds the number of messages still to be produced")
    void batchSizeNeverExceedsRemainingMessages() {
        final var strategy = strategyFor(2L, 10L, Duration.ofMillis(10));

        assertThat(strategy.batchSize()).isEqualTo(2); // min(10, 2 remaining)

        strategy.successfullyProducedOneMessage();
        assertThat(strategy.batchSize()).isEqualTo(1); // min(10, 1 remaining)
    }

    @Test
    @DisplayName("A messageCount below 1 is floored to a single message")
    void messageCountIsFlooredToOne() {
        // interval present so this is not a "once" producer; messageCount 0 is floored to 1.
        final var strategy = strategyFor(0L, null, Duration.ofMillis(10));

        assertThat(strategy.shouldReschedule()).isTrue();
        strategy.successfullyProducedOneMessage();
        assertThat(strategy.shouldReschedule()).isFalse();
    }

    @Test
    @DisplayName("Interval defaults to zero when only a messageCount is configured")
    void intervalDefaultsToZeroWhenOnlyCountConfigured() {
        final var strategy = strategyFor(5L, null, null);
        assertThat(strategy.interval()).isEqualTo(Duration.ZERO);
    }
}
