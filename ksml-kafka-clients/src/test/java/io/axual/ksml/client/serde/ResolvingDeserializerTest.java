package io.axual.ksml.client.serde;

/*-
 * ========================LICENSE_START=================================
 * KSML Kafka clients
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

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.ByteBuffer;
import java.util.Map;

import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class ResolvingDeserializerTest {
    private static final String RESOLVED_TOPIC = "tenant-orders";
    private static final Map<String, Object> CONFIGS = Map.of("axual.topic.pattern", "{tenant}-{topic}", "tenant", "tenant");

    @Mock
    private Deserializer<String> backingDeserializer;

    @Test
    @DisplayName("deserialize resolves the topic before delegating to the backing deserializer")
    void deserializeResolvesTopic() {
        final var deserializer = new ResolvingDeserializer<>(backingDeserializer, CONFIGS);
        final var data = new byte[]{1, 2, 3};

        deserializer.deserialize("orders", data);

        verify(backingDeserializer).deserialize(RESOLVED_TOPIC, data);
    }

    @Test
    @DisplayName("deserialize with headers resolves the topic before delegating")
    void deserializeWithHeadersResolvesTopic() {
        final var deserializer = new ResolvingDeserializer<>(backingDeserializer, CONFIGS);
        final var headers = new RecordHeaders();
        final var data = new byte[]{1, 2, 3};

        deserializer.deserialize("orders", headers, data);

        verify(backingDeserializer).deserialize(RESOLVED_TOPIC, headers, data);
    }

    @Test
    @DisplayName("deserialize with a byte buffer resolves the topic before delegating")
    void deserializeWithByteBufferResolvesTopic() {
        final var deserializer = new ResolvingDeserializer<>(backingDeserializer, CONFIGS);
        final var headers = new RecordHeaders();
        final var data = ByteBuffer.wrap(new byte[]{1, 2, 3});

        deserializer.deserialize("orders", headers, data);

        verify(backingDeserializer).deserialize(RESOLVED_TOPIC, headers, data);
    }

    @Test
    @DisplayName("configure delegates to the backing deserializer")
    void configureDelegates() {
        final var deserializer = new ResolvingDeserializer<>(backingDeserializer, CONFIGS);

        deserializer.configure(CONFIGS, false);

        verify(backingDeserializer).configure(CONFIGS, false);
    }

    @Test
    @DisplayName("close delegates to the backing deserializer")
    void closeDelegates() {
        final var deserializer = new ResolvingDeserializer<>(backingDeserializer, CONFIGS);

        deserializer.close();

        verify(backingDeserializer).close();
    }
}
