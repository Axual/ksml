package io.axual.ksml.data.notation.avro.apicurio;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library - AVRO Apicurio
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

import io.apicurio.registry.rest.client.RegistryClient;
import io.axual.ksml.data.exception.SchemaException;
import io.axual.ksml.data.notation.NotationContext;
import io.axual.ksml.data.schema.StructSchema;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ApicurioAvroNotationTest {

    @Test
    @DisplayName("supportsRemoteSchema returns true when registry client is configured")
    void supportsRemoteSchema_withClient_returnsTrue() {
        var registryClient = mock(RegistryClient.class);
        var provider = new ApicurioAvroNotationProvider(registryClient);
        var context = new NotationContext();
        var notation = (ApicurioAvroNotation) provider.createNotation(context);

        assertThat(notation.supportsRemoteSchema()).isTrue();
    }

    @Test
    @DisplayName("supportsRemoteSchema returns false when no registry client is configured")
    void supportsRemoteSchema_withoutClient_returnsFalse() {
        var provider = new ApicurioAvroNotationProvider();
        var context = new NotationContext();
        var notation = (ApicurioAvroNotation) provider.createNotation(context);

        assertThat(notation.supportsRemoteSchema()).isFalse();
    }

    @Test
    @DisplayName("fetchRemoteSchema fetches and parses schema from registry")
    void fetchRemoteSchema_withValidSubject_returnsSchema() {
        var schemaString = "{\"type\":\"record\",\"name\":\"SensorData\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}";
        var registryClient = mock(RegistryClient.class);
        when(registryClient.getLatestArtifact(null, "my-topic-value"))
                .thenReturn(new ByteArrayInputStream(schemaString.getBytes()));

        var provider = new ApicurioAvroNotationProvider(registryClient);
        var context = new NotationContext();
        var notation = (ApicurioAvroNotation) provider.createNotation(context);

        var schema = notation.fetchRemoteSchema("my-topic", false);

        assertThat(schema).isInstanceOf(StructSchema.class);
        assertThat(((StructSchema) schema).name()).isEqualTo("SensorData");
    }

    @Test
    @DisplayName("fetchRemoteSchema throws SchemaException when registry is unreachable")
    void fetchRemoteSchema_withUnreachableRegistry_throwsSchemaException() {
        var registryClient = mock(RegistryClient.class);
        when(registryClient.getLatestArtifact(null, "unknown-topic-value"))
                .thenThrow(new RuntimeException("Connection refused"));

        var provider = new ApicurioAvroNotationProvider(registryClient);
        var context = new NotationContext();
        var notation = (ApicurioAvroNotation) provider.createNotation(context);

        assertThatThrownBy(() -> notation.fetchRemoteSchema("unknown-topic", false))
                .isInstanceOf(SchemaException.class)
                .hasMessageContaining("unknown-topic-value");
    }

    @Test
    @DisplayName("fetchRemoteSchema throws SchemaException when no registry client configured")
    void fetchRemoteSchema_withoutClient_throwsSchemaException() {
        var provider = new ApicurioAvroNotationProvider();
        var context = new NotationContext();
        var notation = (ApicurioAvroNotation) provider.createNotation(context);

        assertThatThrownBy(() -> notation.fetchRemoteSchema("my-topic", false))
                .isInstanceOf(SchemaException.class)
                .hasMessageContaining("no schema registry client");
    }
}
