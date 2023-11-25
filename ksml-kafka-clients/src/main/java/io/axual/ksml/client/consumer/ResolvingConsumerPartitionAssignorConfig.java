package io.axual.ksml.client.consumer;

/*-
 * ========================LICENSE_START=================================
 * Extended Kafka clients for KSML
 * %%
 * Copyright (C) 2021 - 2023 Axual B.V.
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

import io.axual.ksml.client.generic.ResolvingClientConfig;
import io.axual.ksml.client.resolving.TopicResolver;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;

import java.util.Map;

public class ResolvingConsumerPartitionAssignorConfig extends ResolvingClientConfig {
    private static final String CONFIG_PREFIX = "resolvingconsumerpartitionassignor.";
    public static final String BACKING_ASSIGNOR_CONFIG = CONFIG_PREFIX + "backing.assignor";
    public static final String TOPIC_RESOLVER_CONFIG = CONFIG_PREFIX + "topic.resolver";
    private final ConsumerPartitionAssignor backingAssignor;
    private final TopicResolver topicResolver;

    public ResolvingConsumerPartitionAssignorConfig(Map<String, Object> configs) {
        super(configs);
        downstreamConfigs.remove(BACKING_ASSIGNOR_CONFIG);
        downstreamConfigs.remove(TOPIC_RESOLVER_CONFIG);
        this.backingAssignor = this.getConfiguredInstance(BACKING_ASSIGNOR_CONFIG, ConsumerPartitionAssignor.class);
        this.topicResolver = this.getConfiguredInstance(TOPIC_RESOLVER_CONFIG, TopicResolver.class);
    }

    public ConsumerPartitionAssignor getBackingAssignor() {
        return backingAssignor;
    }

    public TopicResolver getTopicResolver() {
        return topicResolver;
    }
}
