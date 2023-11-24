package io.axual.ksml.client.serde;

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

import io.axual.ksml.client.generic.ResolvingClientConfig;
import io.axual.ksml.client.resolving.TopicResolver;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class ResolvingSerializer<T> implements Serializer<T> {
    private final Serializer<T> backingSerializer;
    private TopicResolver topicResolver;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        backingSerializer.configure(configs, isKey);
        this.topicResolver = new ResolvingClientConfig(configs).getTopicResolver();
    }

    public ResolvingSerializer(Serializer<T> backingSerializer, Map<String, ?> configs) {
        this.backingSerializer = backingSerializer;
        this.topicResolver = new ResolvingClientConfig(configs).getTopicResolver();
    }

    @Override
    public byte[] serialize(String topic, T object) {
        return backingSerializer.serialize(topicResolver != null ? topicResolver.resolve(topic) : topic, object);
    }

    @Override
    public byte[] serialize(String topic, Headers headers, T object) {
        return backingSerializer.serialize(topicResolver != null ? topicResolver.resolve(topic) : topic, headers, object);
    }

    @Override
    public void close() {
        backingSerializer.close();
    }
}
