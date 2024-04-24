package io.axual.ksml.serde;

/*-
 * ========================LICENSE_START=================================
 * KSML
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

import io.axual.ksml.data.mapper.KafkaStreamsDataObjectMapper;
import io.axual.ksml.data.schema.KafkaStreamsSchemaMapper;
import io.axual.ksml.data.serde.DataObjectDeserializer;
import io.axual.ksml.data.serde.DataObjectSerializer;
import io.axual.ksml.data.type.DataType;
import lombok.Getter;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

@Getter
public class StoreSerde implements Serde<Object> {
    private static final KafkaStreamsSchemaMapper SCHEMA_MAPPER = new KafkaStreamsSchemaMapper();
    private static final KafkaStreamsDataObjectMapper DO_MAPPER = new KafkaStreamsDataObjectMapper(true);
    private final Serializer<Object> serializer;
    private final Deserializer<Object> deserializer;

    public StoreSerde(DataType type, Serde<Object> defaultSerde, String topicName) {
        try (final var internalSerializer = new DataObjectSerializer(type, SCHEMA_MAPPER);
             final var internalDeserializer = new DataObjectDeserializer(type, SCHEMA_MAPPER)) {

            // Here we define the serialization logic for state stores. The rule is: if we are serializing to a Kafka
            // topic, then we follow the explicitly provided type and use the corresponding serde. If we are serializing
            // to an in memory or RocksDB store, then we use an internal serde, which prevents external side effects,
            // such as registering an AVRO schema for a non-existing topic. The internal serde is capable of serializing
            // an in-memory DataObject to a byte[] and restoring it from the byte[] completely, including its schema,
            // without any unwanted SR side effects.

            serializer = (topic, data) -> {
                // If we are serializing to the topic, then use the normal serializer
                if (topicName != null && topicName.equals(topic)) {
                    return defaultSerde.serializer().serialize(topic, data);
                }
                // Else use the internal DataObject serializer
                return internalSerializer.serialize(topic, DO_MAPPER.toDataObject(data));
            };

            deserializer = (topic, data) -> {
                // If we are serializing to the topic, then use the normal deserializer
                if (topicName != null && topicName.equals(topic)) {
                    return defaultSerde.deserializer().deserialize(topic, data);
                }
                // Else use the internal DataObject deserializer
                final var dataObject = internalDeserializer.deserialize(topic, data);
                // For compatibility reasons, we have to return native object format here. Otherwise, state stores assisting in
                // count() operations do not get back expected Long types, for example.
                return DO_MAPPER.fromDataObject(dataObject);
            };
        }
    }
}
