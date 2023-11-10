package io.axual.ksml.client.admin;

/*-
 * ========================LICENSE_START=================================
 * axual-client-proxy
 * %%
 * Copyright (C) 2020 Axual B.V.
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


import io.axual.ksml.client.resolving.TopicResolver;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ExtendableCreateTopicsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Uuid;

import java.util.HashMap;
import java.util.Map;

public class ResolvingCreateTopicsResult extends ExtendableCreateTopicsResult {
    private final TopicResolver topicResolver;
    final Map<String, KafkaFuture<Void>> values;

    ResolvingCreateTopicsResult(CreateTopicsResult result, TopicResolver resolver) {
        super(result);
        this.topicResolver = resolver;
        this.values = new HashMap<>();
        result.values().forEach((k, v) -> values.put(topicResolver.unresolveTopic(k), v));
    }

    @Override
    public Map<String, KafkaFuture<Void>> values() {
        return values;
    }

    @Override
    public KafkaFuture<Config> config(String topic) {
        return super.config(topicResolver.resolveTopic(topic));
    }

    @Override
    public KafkaFuture<Uuid> topicId(String topic) {
        return super.topicId(topicResolver.resolveTopic(topic));
    }

    @Override
    public KafkaFuture<Integer> numPartitions(String topic) {
        return super.numPartitions(topicResolver.resolveTopic(topic));
    }

    @Override
    public KafkaFuture<Integer> replicationFactor(String topic) {
        return super.replicationFactor(topicResolver.resolveTopic(topic));
    }
}
