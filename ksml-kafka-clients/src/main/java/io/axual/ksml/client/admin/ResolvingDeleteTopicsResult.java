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
import org.apache.kafka.clients.admin.ExtendableDeleteTopicsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Uuid;

import java.util.HashMap;
import java.util.Map;

public class ResolvingDeleteTopicsResult extends ExtendableDeleteTopicsResult {
    private final TopicResolver topicResolver;

    ResolvingDeleteTopicsResult(Map<Uuid, KafkaFuture<Void>> topicIdFutures, Map<String, KafkaFuture<Void>> nameFutures, TopicResolver topicResolver) {
        super(topicIdFutures, nameFutures);
        this.topicResolver = topicResolver;
    }

    @Override
    public Map<String, KafkaFuture<Void>> topicNameValues() {
        var superResult = super.topicNameValues();
        var result = new HashMap<String, KafkaFuture<Void>>(superResult.size());
        superResult.forEach((topicName, future) -> result.put(topicResolver.unresolve(topicName), future));
        return result;
    }
}
