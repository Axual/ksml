package io.axual.ksml.client.admin;

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

import io.axual.ksml.client.resolving.TopicResolver;
import org.apache.kafka.clients.admin.ExtendableListTopicsResult;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.KafkaFuture;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ResolvingListTopicsResult extends ExtendableListTopicsResult {
    private final TopicResolver resolver;

    public ResolvingListTopicsResult(KafkaFuture<Map<String, TopicListing>> future, final TopicResolver resolver) {
        super(future);
        this.resolver = resolver;
    }

    @Override
    public KafkaFuture<Map<String, TopicListing>> namesToListings() {
        return super.namesToListings().thenApply(rawResult -> {
            Map<String, TopicListing> result = new HashMap<>();
            for (var entry : rawResult.entrySet()) {
                var unresolvedTopic = entry.getValue().isInternal() ? entry.getKey() : resolver.unresolve(entry.getKey());
                if (unresolvedTopic != null) {
                    result.put(unresolvedTopic, new TopicListing(unresolvedTopic, entry.getValue().topicId(), entry.getValue().isInternal()));
                }
            }
            return result;
        });
    }

    @Override
    public KafkaFuture<Collection<TopicListing>> listings() {
        return namesToListings().thenApply(Map::values);
    }

    @Override
    public KafkaFuture<Set<String>> names() {
        return namesToListings().thenApply(Map::keySet);
    }
}
