package io.axual.ksml.data.notation.jsonschema.apicurio;

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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ApicurioJsonSchemaSerdeSupplierTest {
    @Test
    void testSuppliedPropertiesNotOverwritten() {
        // Set up mocks
        Serializer<Object> delegateSerializer = mock(Serializer.class);
        Deserializer<Object> delegateDeserializer = mock(Deserializer.class);
        ArgumentCaptor<Map<String, Object>> configCaptor = ArgumentCaptor.forClass(Map.class);
        // Set up the serde we want to test
        final var serde = new ApicurioJsonSchemaSerdeSupplier.ApicurioJsonSchemaSerde(Serdes.serdeFrom(delegateSerializer, delegateDeserializer));
        // Call the serde with prefilled config map
        final var configs = new HashMap<String, String>();
        configs.put("apicurio.registry.artifact-resolver-strategy", "myconfig");
        configs.putIfAbsent("apicurio.registry.headers.enabled", "myconfig");
        configs.putIfAbsent("apicurio.registry.as-confluent", "myconfig");
        configs.putIfAbsent("apicurio.registry.use-id", "myconfig");
        configs.putIfAbsent("apicurio.registry.serdes.json-schema.validation-enabled", "false");
        serde.configure(configs, false);
        // Capture the config map passed into the delegate serializer
        verify(delegateSerializer).configure(configCaptor.capture(), any(Boolean.class));
        final var modifiedConfigs = configCaptor.getValue();
        assertEquals("myconfig", modifiedConfigs.get("apicurio.registry.artifact-resolver-strategy"));
        assertEquals("myconfig", modifiedConfigs.get("apicurio.registry.headers.enabled"));
        assertEquals("myconfig", modifiedConfigs.get("apicurio.registry.as-confluent"));
        assertEquals("myconfig", modifiedConfigs.get("apicurio.registry.use-id"));
        assertEquals("false", modifiedConfigs.get("apicurio.registry.serdes.json-schema.validation-enabled"));
    }

    @Test
    void testSuppliedPropertiesExtendedWithDefaults() {
        // Set up mocks
        var headerFilterSerde = mock(HeaderFilterSerde.class);
        Serializer<Object> delegateSerializer = mock(Serializer.class);
        Deserializer<Object> delegateDeserializer = mock(Deserializer.class);
        ArgumentCaptor<Map<String, Object>> configCaptor = ArgumentCaptor.forClass(Map.class);
        when(headerFilterSerde.serializer()).thenReturn(delegateSerializer);
        when(headerFilterSerde.deserializer()).thenReturn(delegateDeserializer);
        // Set up the serde we want to test
        final var serde = new ApicurioJsonSchemaSerdeSupplier.ApicurioJsonSchemaSerde(Serdes.serdeFrom(delegateSerializer, delegateDeserializer));
        // Call the serde with empty config map
        serde.configure(new HashMap<String, String>(), false);
        // Capture the config map passed into the delegate serializer
        verify(delegateSerializer).configure(configCaptor.capture(), any(Boolean.class));
        // Verify whether the serializer's configure was called with empty config map
        assertFalse(configCaptor.getValue().isEmpty(), "Expected a non-empty map to be passed to delegate serializer");
        final var modifiedConfigs = configCaptor.getValue();
        // Validate if default properties were added to the empty config map
        assertEquals("io.apicurio.registry.serde.strategy.TopicIdStrategy", modifiedConfigs.get("apicurio.registry.artifact-resolver-strategy"));
        assertEquals(false, modifiedConfigs.get("apicurio.registry.headers.enabled"), "Expected default config enabling payload encoding");
        assertEquals(true, modifiedConfigs.get("apicurio.registry.as-confluent"), "Expected default config enabling Confluent compatibility");
        assertEquals(true, modifiedConfigs.get("apicurio.registry.serdes.json-schema.validation-enabled"), "Expected default config enabling json schema validation");
        assertEquals("contentId", modifiedConfigs.get("apicurio.registry.use-id"), "Expected default config using contentId as schema id");
        // New: default id-handler should be injected as well
        assertEquals("io.apicurio.registry.serde.Legacy4ByteIdHandler", modifiedConfigs.get("apicurio.registry.id-handler"), "Expected default id handler to be set");
    }

    @Test
    void testUserProvidedIdHandlerPreserved() {
        // Set up mocks
        Serializer<Object> delegateSerializer = mock(Serializer.class);
        Deserializer<Object> delegateDeserializer = mock(Deserializer.class);
        ArgumentCaptor<Map<String, Object>> configCaptor = ArgumentCaptor.forClass(Map.class);
        final var serde = new ApicurioJsonSchemaSerdeSupplier.ApicurioJsonSchemaSerde(Serdes.serdeFrom(delegateSerializer, delegateDeserializer));

        // Provide a custom id-handler which should not be overwritten
        final var configs = new HashMap<String, String>();
        configs.put("apicurio.registry.id-handler", "my.custom.IdHandler");
        serde.configure(configs, false);

        verify(delegateSerializer).configure(configCaptor.capture(), any(Boolean.class));
        final var modifiedConfigs = configCaptor.getValue();
        assertEquals("my.custom.IdHandler", modifiedConfigs.get("apicurio.registry.id-handler"));
    }

    @Test
    void testSupplierBasics_vendorNameAndSerdeNotNull() {
        final var supplier = new ApicurioJsonSchemaSerdeSupplier();
        assertEquals("apicurio", supplier.vendorName());
        final var serde = supplier.get(new io.axual.ksml.data.type.StructType(), false);
        assertNotNull(serde);
        assertNotNull(serde.serializer());
        assertNotNull(serde.deserializer());
    }

    @Test
    void testSupplierRegistryClientGetter() {
        final var client = mock(io.apicurio.registry.rest.client.RegistryClient.class);
        final var supplier = new ApicurioJsonSchemaSerdeSupplier(client);
        assertSame(client, supplier.registryClient());
    }
}
