package io.axual.ksml.data.notation.protobuf;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library - Protobuf Confluent
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

import com.google.protobuf.Message;
import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.notation.BaseSerdeProvider;
import io.axual.ksml.data.type.DataType;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import lombok.Getter;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class ConfluentProtobufSerdeProvider extends BaseSerdeProvider implements ProtobufSerdeProvider {
    // Registry Client is mocked by tests
    @Getter
    private final SchemaRegistryClient registryClient;

    public ConfluentProtobufSerdeProvider() {
        this(null);
    }

    public ConfluentProtobufSerdeProvider(SchemaRegistryClient registryClient) {
        super("confluent");
        this.registryClient = registryClient;
    }

    @Override
    public Serde<Object> get(DataType type, boolean isKey) {
        final Serializer<Message> msgSerializer = registryClient != null ? new KafkaProtobufSerializer<>(registryClient) : new KafkaProtobufSerializer<>();
        final Deserializer<Message> msgDeserializer = registryClient != null ? new KafkaProtobufDeserializer<>(registryClient) : new KafkaProtobufDeserializer<>();
        final Serializer<Object> serializer = new Serializer<>() {
            private DataException typeException(Object data) {
                return new DataException("Can not serialize data of type " + data.getClass().getName() + " to Protobuf. Please use a Protobuf data type.");
            }

            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
                msgSerializer.configure(configs, isKey);
            }

            @Override
            public byte[] serialize(String topic, Object data) {
                if (data == null) return msgSerializer.serialize(topic, null);
                if (data instanceof Message msg) return msgSerializer.serialize(topic, msg);
                throw typeException(data);
            }

            @Override
            public byte[] serialize(String topic, Headers headers, Object data) {
                if (data == null) return msgSerializer.serialize(topic, headers, null);
                if (data instanceof Message msg) return msgSerializer.serialize(topic, headers, msg);
                throw typeException(data);
            }
        };
        final Deserializer<Object> deserializer = new Deserializer<>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
                msgDeserializer.configure(configs, isKey);
            }

            @Override
            public Object deserialize(String topic, Headers headers, byte[] data) {
                return msgDeserializer.deserialize(topic, headers, data);
            }

            @Override
            public Object deserialize(String topic, Headers headers, ByteBuffer data) {
                return msgDeserializer.deserialize(topic, headers, data);
            }

            @Override
            public Object deserialize(String topic, byte[] data) {
                return msgDeserializer.deserialize(topic, data);
            }
        };
        return Serdes.serdeFrom(serializer, deserializer);
    }

    @Override
    public ProtobufSchemaParser schemaParser() {
        return new ConfluentProtobufSchemaParser();
    }

    @Override
    public ProtobufDescriptorFileElementMapper fileElementMapper() {
        return new ConfluentProtobufDescriptorFileElementMapper();
    }
}
