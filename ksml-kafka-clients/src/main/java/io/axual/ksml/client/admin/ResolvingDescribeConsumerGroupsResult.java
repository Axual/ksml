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

import io.axual.ksml.client.exception.ClientException;
import io.axual.ksml.client.resolving.GroupResolver;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ExtendableDescribeConsumerGroupsResult;
import org.apache.kafka.common.KafkaFuture;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

@Slf4j
public class ResolvingDescribeConsumerGroupsResult extends ExtendableDescribeConsumerGroupsResult {
    protected final GroupResolver groupResolver;
    final Map<String, KafkaFuture<ConsumerGroupDescription>> describedGroups;

    public ResolvingDescribeConsumerGroupsResult(Map<String, KafkaFuture<ConsumerGroupDescription>> futures, GroupResolver groupResolver) {
        super(futures);
        this.groupResolver = groupResolver;
        describedGroups = new HashMap<>(futures.size());

        futures.forEach((groupId, future) ->
                describedGroups.put(
                        groupResolver.unresolve(groupId),
                        future.thenApply(this::unresolvedConsumerGroupDescription)));
    }

    protected ConsumerGroupDescription unresolvedConsumerGroupDescription(ConsumerGroupDescription description) {
        String unresolvedGroupId = groupResolver.unresolve(description.groupId());
        return new ConsumerGroupDescription(
                unresolvedGroupId,
                description.isSimpleConsumerGroup(),
                description.members(),
                description.partitionAssignor(),
                description.state(),
                description.coordinator()
        );
    }

    @Override
    public Map<String, KafkaFuture<ConsumerGroupDescription>> describedGroups() {
        return describedGroups;
    }

    @Override
    public KafkaFuture<Map<String, ConsumerGroupDescription>> all() {
        return KafkaFuture.allOf(describedGroups.values().toArray(new KafkaFuture[0]))
                .thenApply(unused -> {
                    try {
                        Map<String, ConsumerGroupDescription> allDescriptions = new HashMap<>();
                        for (Entry<String, KafkaFuture<ConsumerGroupDescription>> entry : describedGroups.entrySet()) {
                            allDescriptions.put(entry.getKey(), entry.getValue().get());
                        }
                        return allDescriptions;
                    } catch (InterruptedException | ExecutionException e) {
                        // Should be unreachable because of allOf statement
                        throw new ClientException("Interrupted while waiting for DescribeConsumerGroups", e);
                    }
                });
    }
}
