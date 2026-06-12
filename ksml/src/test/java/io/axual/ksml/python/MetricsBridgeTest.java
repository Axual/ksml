package io.axual.ksml.python;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2024 Axual B.V.
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

import io.axual.ksml.metric.Metrics;
import io.axual.ksml.metric.MetricsRegistry;
import io.axual.ksml.proxy.metric.MetricsBridge;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

@ExtendWith(MockitoExtension.class)
class MetricsBridgeTest {

    @Mock(answer = Answers.RETURNS_MOCKS)
    MetricsRegistry mockedRegistry;

    MetricsBridge metricsBridge;

    @BeforeEach
    void setUp() {
        metricsBridge = new MetricsBridge(Metrics.registry());
    }

    @Test
    void close() {
        var counterBridge = metricsBridge.counter("close-test");
        assertNotNull(counterBridge);
        counterBridge.close();
        var recreated = metricsBridge.counter("close-test");
        assertNotSame(counterBridge, recreated);
    }

    @Test
    void createAndRecreateTimer() {
        var first = metricsBridge.timer("timer-test");
        var second = metricsBridge.timer("timer-test");
        assertSame(first, second);
    }

    @Test
    void counter() {
        var counter = metricsBridge.counter("counter-test");
        assertNotNull(counter);
    }

    @Test
    void meter() {
        var meter = metricsBridge.meter("meter-test");
        assertNotNull(meter);
    }
}
