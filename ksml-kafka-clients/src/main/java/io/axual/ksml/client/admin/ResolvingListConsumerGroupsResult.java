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

import io.axual.ksml.client.resolving.GroupResolver;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ExtendableListConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.common.KafkaFuture;

import java.util.Collection;
import java.util.stream.Collectors;

public class ResolvingListConsumerGroupsResult extends ExtendableListConsumerGroupsResult {

    protected final GroupResolver groupResolver;
    protected final KafkaFuture<Collection<ConsumerGroupListing>> all;
    protected final KafkaFuture<Collection<ConsumerGroupListing>> valid;

    public ResolvingListConsumerGroupsResult(ListConsumerGroupsResult listConsumerGroupsResult, GroupResolver groupResolver) {
        super(listConsumerGroupsResult);
        this.groupResolver = groupResolver;
        all = listConsumerGroupsResult.all().thenApply(this::unresolveConsumerGroupListings);
        valid = listConsumerGroupsResult.valid().thenApply(this::unresolveConsumerGroupListings);
    }

    private Collection<ConsumerGroupListing> unresolveConsumerGroupListings(
            Collection<ConsumerGroupListing> consumerGroupListings) {
        return consumerGroupListings.stream().map(this::unresolveConsumerGroupListings).collect(Collectors.toList());
    }

    private ConsumerGroupListing unresolveConsumerGroupListings(ConsumerGroupListing consumerGroupListing) {
        final String unresolvedGroupId = groupResolver.unresolve(consumerGroupListing.groupId());
        return new ConsumerGroupListing(unresolvedGroupId, consumerGroupListing.isSimpleConsumerGroup(),
                consumerGroupListing.state());
    }

    @Override
    public KafkaFuture<Collection<ConsumerGroupListing>> all() {
        return all;
    }

    @Override
    public KafkaFuture<Collection<ConsumerGroupListing>> valid() {
        return valid;
    }

}
