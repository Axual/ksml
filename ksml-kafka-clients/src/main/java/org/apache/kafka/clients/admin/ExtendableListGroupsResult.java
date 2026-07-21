package org.apache.kafka.clients.admin;

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

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.internals.KafkaFutureImpl;

import java.util.Collection;

/**
 * Base wrapper around a {@link ListGroupsResult} that delegates all futures to a wrapped instance.
 * Lives in this package to access the package-private {@link ListGroupsResult} constructor.
 */
public class ExtendableListGroupsResult extends ListGroupsResult {
    protected final ListGroupsResult listGroupsResult;

    public ExtendableListGroupsResult(ListGroupsResult listGroupsResult) {
        super(new KafkaFutureImpl<>());
        this.listGroupsResult = listGroupsResult;
    }

    @Override
    public KafkaFuture<Collection<GroupListing>> all() {
        return listGroupsResult.all();
    }

    @Override
    public KafkaFuture<Collection<GroupListing>> valid() {
        return listGroupsResult.valid();
    }

    @Override
    public KafkaFuture<Collection<Throwable>> errors() {
        return listGroupsResult.errors();
    }
}
