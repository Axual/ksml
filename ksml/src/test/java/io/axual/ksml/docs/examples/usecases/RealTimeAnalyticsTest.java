package io.axual.ksml.docs.examples.usecases;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2025 Axual B.V.
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

import io.axual.ksml.testutil.KSMLDriver;
import io.axual.ksml.testutil.KSMLTest;
import io.axual.ksml.testutil.KSMLTestExtension;
import io.axual.ksml.testutil.KSMLTopic;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.internals.WrappedStateStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for the real-time analytics use case pipeline. The docs say that every update of the sales per category is
 * written, so its store must not be cached. TopologyTestDriver commits after every record, so the output alone can't
 * show this, and the test checks the store.
 */
@ExtendWith(KSMLTestExtension.class)
@SuppressWarnings("java:S2187")
public class RealTimeAnalyticsTest {

    @KSMLTopic(topic = "ecommerce_transactions")
    TestInputTopic<String, String> transactions;

    @KSMLTopic(topic = "sales_by_category")
    TestOutputTopic<String, String> salesByCategory;

    @KSMLDriver
    TopologyTestDriver driver;

    @KSMLTest(topology = "docs-examples/use-cases/real-time-analytics/analytics.yaml")
    @DisplayName("Every transaction writes an update of the sales of its category")
    void everyTransactionUpdatesSalesByCategory() {
        transactions.pipeInput("t1", "{\"product_category\": \"books\", \"region\": \"EU\", \"price\": 10.0, \"quantity\": 1}", 1000L);
        transactions.pipeInput("t2", "{\"product_category\": \"books\", \"region\": \"EU\", \"price\": 5.0, \"quantity\": 2}", 2000L);

        assertThat(salesByCategory.readValuesToList()).hasSize(2);
        assertThat(isCached(driver.getAllStateStores().get("category_store"))).as("category_store is cached").isFalse();
    }

    private static boolean isCached(StateStore store) {
        StateStore current = store;
        while (current instanceof WrappedStateStore<?, ?, ?> wrapped) {
            if (current.getClass().getSimpleName().startsWith("Caching")) return true;
            current = wrapped.wrapped();
        }
        return false;
    }
}
