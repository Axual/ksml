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

import io.axual.ksml.client.resolving.GroupResolver;
import org.apache.kafka.clients.admin.ExtendableDeleteConsumerGroupsResult;
import org.apache.kafka.common.KafkaFuture;

import java.util.HashMap;
import java.util.Map;

public class ResolvingDeleteConsumerGroupsResult extends ExtendableDeleteConsumerGroupsResult {
    protected final Map<String, KafkaFuture<Void>> unresolvedFutures;

    public ResolvingDeleteConsumerGroupsResult(Map<String, KafkaFuture<Void>> futures, GroupResolver groupResolver) {
        super(futures);
        this.unresolvedFutures = new HashMap<>(futures.size());
        futures.forEach((groupId, future) -> {
            String unresolvedGroupId = groupResolver.unresolve(groupId);
            if (unresolvedGroupId != null) {
                unresolvedFutures.put(unresolvedGroupId, future);
            }
        });
    }

    @Override
    public Map<String, KafkaFuture<Void>> deletedGroups() {
        return unresolvedFutures;
    }
}
