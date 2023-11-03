package io.axual.ksml.client.serde;

/*-
 * ========================LICENSE_START=================================
 * Kafka clients for KSML
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
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class ResolvingDeserializer<T> implements Deserializer<T> {
    private final Deserializer<T> backingDeserializer;
    private TopicResolver topicResolver;

    public ResolvingDeserializer(Deserializer<T> backingDeserializer, Map<String, ?> configs) {
        this.backingDeserializer = backingDeserializer;
        this.topicResolver = new ResolvingClientConfig(configs).getTopicResolver();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        backingDeserializer.configure(configs, isKey);
        this.topicResolver = new ResolvingClientConfig(configs).getTopicResolver();
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        return backingDeserializer.deserialize(topicResolver != null ? topicResolver.resolve(topic) : topic, data);
    }

    @Override
    public T deserialize(String topic, Headers headers, byte[] data) {
        return backingDeserializer.deserialize(topicResolver != null ? topicResolver.resolve(topic) : topic, headers, data);
    }

    @Override
    public T deserialize(String topic, Headers headers, ByteBuffer data) {
        return backingDeserializer.deserialize(topicResolver != null ? topicResolver.resolve(topic) : topic, headers, data);
    }

    @Override
    public void close() {
        backingDeserializer.close();
    }
}
