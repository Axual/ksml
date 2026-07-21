package io.axual.ksml.client.admin;

/*-
 * ========================LICENSE_START=================================
 * Extended Kafka clients for KSML
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

import io.axual.ksml.client.resolving.GroupResolver;
import org.apache.kafka.clients.admin.ExtendableListGroupsResult;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.ListGroupsResult;
import org.apache.kafka.common.KafkaFuture;

import java.util.Collection;

public class ResolvingListGroupsResult extends ExtendableListGroupsResult {

    protected final GroupResolver groupResolver;
    protected final KafkaFuture<Collection<GroupListing>> all;
    protected final KafkaFuture<Collection<GroupListing>> valid;

    public ResolvingListGroupsResult(ListGroupsResult listGroupsResult, GroupResolver groupResolver) {
        super(listGroupsResult);
        this.groupResolver = groupResolver;
        all = listGroupsResult.all().thenApply(this::unresolveGroupListings);
        valid = listGroupsResult.valid().thenApply(this::unresolveGroupListings);
    }

    private Collection<GroupListing> unresolveGroupListings(Collection<GroupListing> groupListings) {
        return groupListings.stream().map(this::unresolveGroupListing).toList();
    }

    private GroupListing unresolveGroupListing(GroupListing groupListing) {
        final String unresolvedGroupId = groupResolver.unresolve(groupListing.groupId());
        return new GroupListing(unresolvedGroupId, groupListing.type(), groupListing.protocol(), groupListing.groupState());
    }

    @Override
    public KafkaFuture<Collection<GroupListing>> all() {
        return all;
    }

    @Override
    public KafkaFuture<Collection<GroupListing>> valid() {
        return valid;
    }

}
