package io.axual.ksml.data.notation.protobuf.apicurio;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library - PROTOBUF Apicurio
 * %%
 * Copyright (C) 2021 - 2025 Axual B.V.
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

import io.axual.ksml.data.serde.HeaderFilterSerde;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ApicurioProtobufSerdeSupplierTest {
    @Test
    void testSuppliedPropertiesNotOverwritten() {
        // Set up mocks
        HeaderFilterSerde headerFilterSerde = mock(HeaderFilterSerde.class);
        Serializer<Object> delegateSerializer = mock(Serializer.class);
        Deserializer<Object> delegateDeserializer = mock(Deserializer.class);
        ArgumentCaptor<Map<String, Object>> configCaptor = ArgumentCaptor.forClass(Map.class);
        when(headerFilterSerde.serializer()).thenReturn(delegateSerializer);
        when(headerFilterSerde.deserializer()).thenReturn(delegateDeserializer);
        // Set up the serde we want to test
        final var serde = new ApicurioProtobufSerdeSupplier.ApicurioProtobufSerde(headerFilterSerde);
        // Call the serde with prefilled config map
        final var configs = new HashMap<String, String>();
        configs.put("apicurio.registry.artifact-resolver-strategy", "myconfig");
        configs.putIfAbsent("apicurio.registry.headers.enabled", "myconfig");
        configs.putIfAbsent("apicurio.registry.as-confluent", "myconfig");
        configs.putIfAbsent("apicurio.registry.use-id", "myconfig");
        serde.configure(configs, false);
        // Capture the config map passed into the delegate serializer
        verify(delegateSerializer).configure(configCaptor.capture(), any(Boolean.class));
        final var modifiedConfigs = configCaptor.getValue();
        assertEquals("myconfig", modifiedConfigs.get("apicurio.registry.artifact-resolver-strategy"));
        assertEquals("myconfig", modifiedConfigs.get("apicurio.registry.headers.enabled"));
        assertEquals("myconfig", modifiedConfigs.get("apicurio.registry.as-confluent"));
        assertEquals("myconfig", modifiedConfigs.get("apicurio.registry.use-id"));
    }
    @Test
    void testSuppliedPropertiesExtendedWithDefaults() {
        // Set up mocks
        HeaderFilterSerde headerFilterSerde = mock(HeaderFilterSerde.class);
        Serializer<Object> delegateSerializer = mock(Serializer.class);
        Deserializer<Object> delegateDeserializer = mock(Deserializer.class);
        ArgumentCaptor<Map<String, Object>> configCaptor = ArgumentCaptor.forClass(Map.class);
        when(headerFilterSerde.serializer()).thenReturn(delegateSerializer);
        when(headerFilterSerde.deserializer()).thenReturn(delegateDeserializer);
        // Set up the serde we want to test
        final var serde = new ApicurioProtobufSerdeSupplier.ApicurioProtobufSerde(headerFilterSerde);
        // Call the serde with empty config map
        serde.configure(new HashMap<String, String>(), false);
        // Capture the config map passed into the delegate serializer
        verify(delegateSerializer).configure(configCaptor.capture(), any(Boolean.class));
        // Verify whether the serializer's configure was called with empty config map
        assertFalse(configCaptor.getValue().isEmpty(), "Expected a non-empty map to be passed to delegate serializer");
        final var modifiedConfigs = configCaptor.getValue();
        // Validate if default properties were added to the empty config map
        assertEquals( "io.apicurio.registry.serde.strategy.TopicIdStrategy", modifiedConfigs.get("apicurio.registry.artifact-resolver-strategy"));
        assertEquals(false, modifiedConfigs.get("apicurio.registry.headers.enabled"), "Expected default config enabling payload encoding");
        assertEquals(true, modifiedConfigs.get("apicurio.registry.as-confluent"), "Expected default config enabling Confluent compatibility");
        assertEquals("contentId", modifiedConfigs.get("apicurio.registry.use-id"), "Expected default config using contentId as schema id");
    }
}
