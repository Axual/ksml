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

import java.util.Collections;
import java.util.Map;

public class ExtendableCreateTopicsResult extends CreateTopicsResult {
    protected final CreateTopicsResult createTopicsResult;

    public ExtendableCreateTopicsResult(CreateTopicsResult createTopicsResult) {
        super(Collections.emptyMap());
        this.createTopicsResult = createTopicsResult == null ? new CreateTopicsResult(Collections.emptyMap()) : createTopicsResult;
    }

    @Override
    public Map<String, KafkaFuture<Void>> values() {
        return createTopicsResult.values();
    }

    @Override
    public KafkaFuture<Void> all() {
        return createTopicsResult.all();
    }

    @Override
    public KafkaFuture<Config> config(String topic) {
        return createTopicsResult.config(topic);
    }

    @Override
    public KafkaFuture<Integer> numPartitions(String topic) {
        return createTopicsResult.numPartitions(topic);
    }

    @Override
    public KafkaFuture<Integer> replicationFactor(String topic) {
        return createTopicsResult.replicationFactor(topic);
    }
}
