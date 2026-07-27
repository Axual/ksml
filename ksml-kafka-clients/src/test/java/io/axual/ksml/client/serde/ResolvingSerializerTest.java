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
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;

import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class ResolvingSerializerTest {
    private static final String RESOLVED_TOPIC = "tenant-orders";
    private static final Map<String, Object> CONFIGS = Map.of("axual.topic.pattern", "{tenant}-{topic}", "tenant", "tenant");

    @Mock
    private Serializer<String> backingSerializer;

    @Test
    @DisplayName("serialize resolves the topic before delegating to the backing serializer")
    void serializeResolvesTopic() {
        final var serializer = new ResolvingSerializer<>(backingSerializer, CONFIGS);

        serializer.serialize("orders", "value");

        verify(backingSerializer).serialize(RESOLVED_TOPIC, "value");
    }

    @Test
    @DisplayName("serialize with headers resolves the topic before delegating")
    void serializeWithHeadersResolvesTopic() {
        final var serializer = new ResolvingSerializer<>(backingSerializer, CONFIGS);
        final var headers = new RecordHeaders();

        serializer.serialize("orders", headers, "value");

        verify(backingSerializer).serialize(RESOLVED_TOPIC, headers, "value");
    }

    @Test
    @DisplayName("configure delegates to the backing serializer")
    void configureDelegates() {
        final var serializer = new ResolvingSerializer<>(backingSerializer, CONFIGS);

        serializer.configure(CONFIGS, true);

        verify(backingSerializer).configure(CONFIGS, true);
    }

    @Test
    @DisplayName("close delegates to the backing serializer")
    void closeDelegates() {
        final var serializer = new ResolvingSerializer<>(backingSerializer, CONFIGS);

        serializer.close();

        verify(backingSerializer).close();
    }
}
