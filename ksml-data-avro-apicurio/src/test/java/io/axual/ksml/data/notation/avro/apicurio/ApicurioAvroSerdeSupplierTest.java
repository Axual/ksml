package io.axual.ksml.data.notation.avro.apicurio;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library - AVRO Apicurio
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

import io.apicurio.registry.resolver.client.RegistryClientFacade;
import io.apicurio.registry.resolver.config.SchemaResolverConfig;
import io.apicurio.registry.serde.Default4ByteIdHandler;
import io.apicurio.registry.serde.config.SerdeConfig;
import io.apicurio.registry.serde.kafka.config.KafkaSerdeConfig;
import io.apicurio.registry.serde.strategy.TopicIdStrategy;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class ApicurioAvroSerdeSupplierTest {

    @Test
    @DisplayName("get() builds a Serde using the default Apicurio serializers when no client is set")
    void getWithoutRegistryClient() {
        assertThat(new ApicurioAvroSerdeSupplier(null).get(null, false)).isNotNull();
    }

    @Test
    @DisplayName("get() builds a Serde around the supplied registry client")
    void getWithRegistryClient() {
        final var client = mock(RegistryClientFacade.class);
        assertThat(new ApicurioAvroSerdeSupplier(client).get(null, true)).isNotNull();
    }

    @Test
    @DisplayName("Pins the Confluent-compatible 4-byte content-id format and find-latest (issue #290)")
    @SuppressWarnings("unchecked")
    void injectsConfluentCompatibleDefaults() {
        final var injected = configureAndCapture(new HashMap<>());
        assertThat(injected)
                .containsEntry(SchemaResolverConfig.ARTIFACT_RESOLVER_STRATEGY, TopicIdStrategy.class.getCanonicalName())
                .containsEntry(KafkaSerdeConfig.ENABLE_HEADERS, false)
                .containsEntry(SerdeConfig.USE_ID, "contentId")
                .containsEntry(SerdeConfig.ID_HANDLER, Default4ByteIdHandler.class.getCanonicalName())
                .containsEntry(SchemaResolverConfig.FIND_LATEST_ARTIFACT, true);
    }

    @Test
    @DisplayName("User-supplied values are never overwritten")
    @SuppressWarnings("unchecked")
    void userValuesPreserved() {
        final Map<String, Object> configs = new HashMap<>();
        configs.put(SerdeConfig.USE_ID, "globalId");
        configs.put(SchemaResolverConfig.FIND_LATEST_ARTIFACT, false);
        final var injected = configureAndCapture(configs);
        assertThat(injected)
                .containsEntry(SerdeConfig.USE_ID, "globalId")
                .containsEntry(SchemaResolverConfig.FIND_LATEST_ARTIFACT, false);
    }

    @Test
    @DisplayName("When the user enables headers, the payload id config is not injected")
    @SuppressWarnings("unchecked")
    void headersEnabledSkipsPayloadIdConfig() {
        final Map<String, Object> configs = new HashMap<>();
        configs.put(KafkaSerdeConfig.ENABLE_HEADERS, true);
        final var injected = configureAndCapture(configs);
        assertThat(injected).doesNotContainKey(SerdeConfig.USE_ID)
        // find-latest is applied regardless of the header mode
                        .containsEntry(SchemaResolverConfig.FIND_LATEST_ARTIFACT, true);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> configureAndCapture(Map<String, Object> configs) {
        final Serializer<Object> serializer = mock(Serializer.class);
        final Deserializer<Object> deserializer = mock(Deserializer.class);
        final var serde = new ApicurioAvroSerdeSupplier.ApicurioAvroSerde(Serdes.serdeFrom(serializer, deserializer));
        serde.configure(configs, false);
        final ArgumentCaptor<Map<String, Object>> captor = ArgumentCaptor.forClass(Map.class);
        verify(serializer).configure(captor.capture(), anyBoolean());
        return captor.getValue();
    }
}
