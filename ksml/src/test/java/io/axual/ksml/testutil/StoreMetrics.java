package io.axual.ksml.testutil;

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

import org.apache.kafka.streams.TopologyTestDriver;

/**
 * Reads state store metrics from a {@link TopologyTestDriver}.
 */
public final class StoreMetrics {
    private StoreMetrics() {
    }

    /**
     * Returns the number of iterators on a store that were created but not closed yet, from the Kafka Streams metric
     * {@code num-open-iterators}.
     *
     * @param driver    the test driver
     * @param storeName the name of the store
     * @return the number of open iterators
     * @throws AssertionError when the store has no such metric
     */
    public static long openIterators(TopologyTestDriver driver, String storeName) {
        return driver.metrics().entrySet().stream()
                .filter(e -> e.getKey().name().equals("num-open-iterators"))
                // Kafka Streams puts the store name in the tag <store type>-state-id
                .filter(e -> e.getKey().tags().entrySet().stream()
                        .anyMatch(t -> t.getKey().endsWith("-state-id") && t.getValue().equals(storeName)))
                .map(e -> ((Number) e.getValue().metricValue()).longValue())
                .findFirst()
                .orElseThrow(() -> new AssertionError("No num-open-iterators metric for store " + storeName));
    }
}
