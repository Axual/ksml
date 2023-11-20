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
import org.apache.kafka.clients.admin.ExtendableDescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Uuid;

import java.util.HashMap;
import java.util.Map;

public class ResolvingDescribeTopicsResult extends ExtendableDescribeTopicsResult {
    public ResolvingDescribeTopicsResult(Map<Uuid, KafkaFuture<TopicDescription>> topicIdFutures,
                                         Map<String, KafkaFuture<TopicDescription>> nameFutures,
                                         TopicResolver resolver) {
        super(unresolveTopicDescriptions(topicIdFutures, resolver),
                unresolveTopicDescriptions(ResolverUtil.unresolveKeys(nameFutures, resolver), resolver));
    }

    /**
     * @deprecated
     */
    @Deprecated
    public ResolvingDescribeTopicsResult(Map<String, KafkaFuture<TopicDescription>> futures, TopicResolver resolver) {
        super(unresolveTopicDescriptions(ResolverUtil.unresolveKeys(futures, resolver), resolver));
    }

    private static <T> Map<T, KafkaFuture<TopicDescription>> unresolveTopicDescriptions(Map<T, KafkaFuture<TopicDescription>> values, TopicResolver resolver) {
        if (values == null) return null;
        Map<T, KafkaFuture<TopicDescription>> result = new HashMap<>();
        values.forEach((key, value) -> result.put(key, value.thenApply(td -> unresolveTopicDescription(td, resolver))));
        return result;
    }

    private static TopicDescription unresolveTopicDescription(TopicDescription td, TopicResolver resolver) {
        return new TopicDescription(resolver.unresolve(td.name()), td.isInternal(), td.partitions(), td.authorizedOperations());
    }
}
