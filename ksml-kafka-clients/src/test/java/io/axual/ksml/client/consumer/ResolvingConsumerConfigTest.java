package io.axual.ksml.client.consumer;

/*-
 * ========================LICENSE_START=================================
 * KSML Kafka clients
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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ResolvingConsumerConfigTest {
    private static final Map<String, Object> PATTERNS = Map.of(
            "axual.topic.pattern", "{tenant}-{topic}",
            "axual.group.id.pattern", "{tenant}-{group.id}",
            "tenant", "tenant");

    @Test
    @DisplayName("A configured group id is resolved into the downstream config")
    void groupIdIsResolved() {
        final Map<String, Object> configs = new HashMap<>(PATTERNS);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, "group");

        final var config = new ResolvingConsumerConfig(configs);

        assertThat(config.downstreamConfigs()).containsEntry(ConsumerConfig.GROUP_ID_CONFIG, "tenant-group");
    }

    @Test
    @DisplayName("A partition assignment strategy is replaced by the resolving assignor")
    void partitionAssignmentStrategyIsWrapped() {
        final Map<String, Object> configs = new HashMap<>(PATTERNS);
        configs.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RangeAssignor.class.getName());

        final var config = new ResolvingConsumerConfig(configs);

        assertThat(config.downstreamConfigs())
                .containsEntry(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, ResolvingConsumerPartitionAssignor.class.getName())
                .containsEntry(ResolvingConsumerPartitionAssignorConfig.BACKING_ASSIGNOR_CONFIG, RangeAssignor.class.getName())
                .containsEntry(ResolvingConsumerPartitionAssignorConfig.ASSIGNOR_TOPIC_RESOLVER_CONFIG, config.topicResolver());
    }

    @Test
    @DisplayName("Without a group id or assignment strategy the downstream config is unchanged")
    void nothingToRewrite() {
        final var config = new ResolvingConsumerConfig(new HashMap<>(PATTERNS));

        assertThat(config.downstreamConfigs())
                .doesNotContainKey(ConsumerConfig.GROUP_ID_CONFIG)
                .doesNotContainKey(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG);
    }
}
