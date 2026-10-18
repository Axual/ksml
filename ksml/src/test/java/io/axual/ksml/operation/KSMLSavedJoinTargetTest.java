package io.axual.ksml.operation;

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

import io.axual.ksml.testutil.KSMLTest;
import io.axual.ksml.testutil.KSMLTestExtension;
import io.axual.ksml.testutil.KSMLTopic;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

// Joins and merges with a stream or table that an earlier pipeline saved with "as" (GitHub issue 286)
@ExtendWith(KSMLTestExtension.class)
class KSMLSavedJoinTargetTest {

    // test-join-saved-table.yaml
    @KSMLTopic(topic = "table_j1_topic")
    TestInputTopic<String, String> tableJ1;

    @KSMLTopic(topic = "table_j2_topic")
    TestInputTopic<String, String> tableJ2;

    @KSMLTopic(topic = "output_topic")
    TestOutputTopic<String, String> output;

    // test-join-saved-table-chain.yaml
    @KSMLTopic(topic = "customers_topic")
    TestInputTopic<String, String> customers;

    @KSMLTopic(topic = "accounts_topic")
    TestInputTopic<String, String> accounts;

    @KSMLTopic(topic = "transactions_topic")
    TestInputTopic<String, String> transactions;

    @KSMLTopic(topic = "order_topic")
    TestInputTopic<String, String> orders;

    @KSMLTopic(topic = "enriched_orders_topic")
    TestOutputTopic<String, String> enrichedOrders;

    // test-join-saved-table-reused.yaml
    @KSMLTopic(topic = "products_topic")
    TestInputTopic<String, String> products;

    @KSMLTopic(topic = "inventory_topic")
    TestInputTopic<String, String> inventory;

    @KSMLTopic(topic = "web_orders_topic")
    TestInputTopic<String, String> webOrders;

    @KSMLTopic(topic = "mobile_orders_topic")
    TestInputTopic<String, String> mobileOrders;

    @KSMLTopic(topic = "processed_web_orders_topic")
    TestOutputTopic<String, String> processedWebOrders;

    @KSMLTopic(topic = "processed_mobile_orders_topic")
    TestOutputTopic<String, String> processedMobileOrders;

    // test-join-saved-stream.yaml
    @KSMLTopic(topic = "left_topic")
    TestInputTopic<String, String> left;

    @KSMLTopic(topic = "right_topic")
    TestInputTopic<String, String> right;

    @KSMLTopic(topic = "join_topic")
    TestOutputTopic<String, String> joined;

    @KSMLTopic(topic = "merge_topic")
    TestOutputTopic<String, String> merged;

    // test-join-saved-stream.yaml and test-join-saved-table-left-outer.yaml
    @KSMLTopic(topic = "input_topic")
    TestInputTopic<String, String> input;

    @KSMLTopic(topic = "leftjoin_topic")
    TestOutputTopic<String, String> leftJoined;

    @KSMLTopic(topic = "outerjoin_topic")
    TestOutputTopic<String, String> outerJoined;

    // test-join-saved-table-left-outer.yaml
    @KSMLTopic(topic = "updates_topic")
    TestInputTopic<String, String> updates;

    @KSMLTopic(topic = "input_table_topic")
    TestInputTopic<String, String> inputTable;

    @KSMLTest(topology = "pipelines/test-join-saved-table.yaml")
    @DisplayName("a stream joins with a table that an earlier table-table join saved with \"as\"")
    void testJoinWithSavedTable() {
        tableJ1.pipeInput("key1", "a");
        tableJ2.pipeInput("key1", "b");
        input.pipeInput("key1", "x");
        input.pipeInput("key2", "y");

        assertThat(output.readKeyValuesToList()).containsExactly(new KeyValue<>("key1", "x+a+b"));
    }

    @KSMLTest(topology = "pipelines/test-join-saved-table-chain.yaml")
    @DisplayName("a stream joins with the last of two table-table joins, each saved with \"as\"")
    void testJoinWithSavedTableChain() {
        customers.pipeInput("cust1", "customer");
        accounts.pipeInput("cust1", "account");
        transactions.pipeInput("cust1", "transaction");
        orders.pipeInput("cust1", "order");

        assertThat(enrichedOrders.readKeyValuesToList()).containsExactly(
                new KeyValue<>("cust1", "order+customer+account+transaction"));
    }

    @KSMLTest(topology = "pipelines/test-join-saved-table-reused.yaml")
    @DisplayName("two pipelines join with the same table saved with \"as\"")
    void testJoinWithReusedSavedTable() {
        products.pipeInput("prod1", "product");
        inventory.pipeInput("prod1", "5");
        webOrders.pipeInput("prod1", "web");
        mobileOrders.pipeInput("prod1", "mobile");

        assertThat(processedWebOrders.readKeyValuesToList()).containsExactly(new KeyValue<>("prod1", "web+product+5"));
        assertThat(processedMobileOrders.readKeyValuesToList()).containsExactly(new KeyValue<>("prod1", "mobile+product+5"));
    }

    @KSMLTest(topology = "pipelines/test-join-saved-stream.yaml")
    @DisplayName("join, leftJoin, outerJoin and merge use a stream saved with \"as\", not the topic it came from")
    void testJoinsAndMergeWithSavedStream() {
        final var time = Instant.ofEpochMilli(60_000);
        right.pipeInput("key1", "skip", time);
        right.pipeInput("key1", "r", time);
        left.pipeInput("key1", "l", time);

        assertThat(joined.readKeyValuesToList()).containsExactly(new KeyValue<>("key1", "l+r"));
        assertThat(leftJoined.readKeyValuesToList()).containsExactly(new KeyValue<>("key1", "l+r"));
        // An outer join may also emit unmatched records when the window closes, so only check the match
        assertThat(outerJoined.readValuesToList()).contains("l+r").noneMatch(value -> value.contains("skip"));
        assertThat(merged.readKeyValuesToList()).containsExactly(
                new KeyValue<>("key1", "r"),
                new KeyValue<>("key1", "l"));
    }

    @KSMLTest(topology = "pipelines/test-join-saved-table-left-outer.yaml")
    @DisplayName("leftJoin and outerJoin use a table saved with \"as\"")
    void testLeftAndOuterJoinWithSavedTable() {
        updates.pipeInput("key1", "u");
        input.pipeInput("key1", "i");
        input.pipeInput("key2", "j");
        inputTable.pipeInput("key1", "t");

        assertThat(leftJoined.readKeyValuesToList()).containsExactly(
                new KeyValue<>("key1", "i+u"),
                new KeyValue<>("key2", "j+None"));
        assertThat(outerJoined.readKeyValuesToList()).containsExactly(
                new KeyValue<>("key1", "None+u"),
                new KeyValue<>("key1", "t+u"));
    }
}
