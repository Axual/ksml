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

import static org.assertj.core.api.Assertions.assertThat;

// Table-table joins with a foreignKeyExtractor and an otherPartitioner, where the two tables have different key types
@ExtendWith(KSMLTestExtension.class)
class KSMLForeignKeyJoinTest {

    @KSMLTopic(topic = "orders_topic", keySerde = KSMLTopic.SerdeType.LONG)
    TestInputTopic<Long, String> orders;

    @KSMLTopic(topic = "customers_topic")
    TestInputTopic<String, String> customers;

    @KSMLTopic(topic = "joined_orders_topic", keySerde = KSMLTopic.SerdeType.LONG)
    TestOutputTopic<Long, String> joined;

    @KSMLTopic(topic = "left_joined_orders_topic", keySerde = KSMLTopic.SerdeType.LONG)
    TestOutputTopic<Long, String> leftJoined;

    @KSMLTest(topology = "pipelines/test-join-foreign-key.yaml")
    @DisplayName("join and leftJoin find the other table's row by the key that the foreignKeyExtractor returns, with an otherPartitioner for that key")
    void testForeignKeyJoins() {
        customers.pipeInput("cust1", "Alice");
        orders.pipeInput(1L, "{\"customer_id\": \"cust1\", \"item\": \"book\"}");
        orders.pipeInput(2L, "{\"customer_id\": \"cust2\", \"item\": \"pen\"}");

        assertThat(joined.readKeyValuesToList()).containsExactly(new KeyValue<>(1L, "book@Alice"));
        assertThat(leftJoined.readKeyValuesToList()).containsExactly(
                new KeyValue<>(1L, "book@Alice"),
                new KeyValue<>(2L, "pen@unknown"));
    }
}
