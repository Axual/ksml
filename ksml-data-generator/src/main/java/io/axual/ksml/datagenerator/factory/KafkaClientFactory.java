package io.axual.ksml.datagenerator.factory;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Generator
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

import io.axual.ksml.client.producer.ResolvingProducer;
import io.axual.ksml.client.serde.ResolvingSerializer;
import io.axual.ksml.notation.NotationLibrary;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;

public class KafkaClientFactory implements ClientFactory {
    private final Map<String, Object> clientConfigs;
    private final NotationLibrary notationLibrary;

    public KafkaClientFactory(Map<String, String> configs) {
        this.clientConfigs = new HashMap<>(configs);
        notationLibrary = new NotationLibrary(this.clientConfigs);
    }

    @Override
    public Producer<byte[], byte[]> getProducer() {
        return new ResolvingProducer<>(clientConfigs);
    }

    public <T> Serializer<T> wrapSerializer(Serializer<T> serializer) {
        return new ResolvingSerializer<>(serializer, clientConfigs);
    }

    @Override
    public NotationLibrary getNotationLibrary() {
        return notationLibrary;
    }
}
