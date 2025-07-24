package io.axual.ksml.data.notation.protobuf;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library - Protobuf Apicurio
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
import io.apicurio.registry.serde.SerdeConfig;
import io.apicurio.registry.serde.SerdeHeaders;
import io.apicurio.registry.serde.protobuf.ProtobufKafkaDeserializer;
import io.apicurio.registry.serde.protobuf.ProtobufKafkaSerializer;
import io.axual.ksml.data.notation.BaseSerdeProvider;
import io.axual.ksml.data.serde.HeaderFilterSerde;
import io.axual.ksml.data.type.DataType;
import lombok.Getter;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;
import java.util.Set;

public class ApicurioProtobufSerdeProvider extends BaseSerdeProvider implements ProtobufSerdeProvider {
    // Registry Client is mocked by tests
    @Getter
    private final RegistryClient registryClient;

    public ApicurioProtobufSerdeProvider() {
        this(null);
    }

    public ApicurioProtobufSerdeProvider(RegistryClient registryClient) {
        super("apicurio");
        this.registryClient = registryClient;
    }

    @Override
    public Serde<Object> get(DataType type, boolean isKey) {
        return new Serde<>() {
            private final HeaderFilterSerde serde = new HeaderFilterSerde(
                    (Serde) Serdes.serdeFrom(
                            registryClient != null ? new ProtobufKafkaSerializer<>(registryClient) : new ProtobufKafkaSerializer<>(),
                            registryClient != null ? new ProtobufKafkaDeserializer<>(registryClient) : new ProtobufKafkaDeserializer<>()));

            @Override
            public Serializer<Object> serializer() {
                return serde.serializer();
            }

            @Override
            public Deserializer<Object> deserializer() {
                return serde.deserializer();
            }

            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
                final String messageTypeHeaderName;
                if (isKey) {
                    messageTypeHeaderName = (String) ((Map<String, Object>) configs).getOrDefault(SerdeConfig.HEADER_KEY_MESSAGE_TYPE_OVERRIDE_NAME, SerdeHeaders.HEADER_KEY_MESSAGE_TYPE);
                } else {
                    messageTypeHeaderName = (String) ((Map<String, Object>) configs).getOrDefault(SerdeConfig.HEADER_VALUE_MESSAGE_TYPE_OVERRIDE_NAME, SerdeHeaders.HEADER_VALUE_MESSAGE_TYPE);
                }
                serde.filteredHeaders(Set.of(messageTypeHeaderName));
                serde.configure(configs, isKey);
            }
        };
    }

    @Override
    public ProtobufSchemaParser schemaParser() {
        return new ApicurioProtobufSchemaParser();
    }

    @Override
    public ProtobufDescriptorFileElementMapper fileElementMapper() {
        return new ApicurioProtobufDescriptorFileElementMapper();
    }
}
