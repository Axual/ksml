package org.apache.kafka.clients.admin;

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

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.KafkaFutureImpl;

import java.util.Collections;

public class ExtendableDeleteConsumerGroupOffsetsResult extends DeleteConsumerGroupOffsetsResult {
    protected final DeleteConsumerGroupOffsetsResult deleteConsumerGroupOffsetsResult;

    public ExtendableDeleteConsumerGroupOffsetsResult(
            DeleteConsumerGroupOffsetsResult deleteConsumerGroupOffsetsResult) {
        super(new KafkaFutureImpl<>(), Collections.emptySet());
        this.deleteConsumerGroupOffsetsResult = deleteConsumerGroupOffsetsResult;
    }

    @Override
    public KafkaFuture<Void> partitionResult(TopicPartition partition) {
        return deleteConsumerGroupOffsetsResult.partitionResult(partition);
    }

    @Override
    public KafkaFuture<Void> all() {
        return deleteConsumerGroupOffsetsResult.all();
    }
}
