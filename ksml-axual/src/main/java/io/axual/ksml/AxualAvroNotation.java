package io.axual.ksml;

/*-
 * ========================LICENSE_START=================================
 * KSML for Axual
 * %%
 * Copyright (C) 2021 Axual B.V.
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

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;

import io.axual.ksml.avro.AvroDataMapper;
import io.axual.ksml.data.mapper.DataObjectMapper;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.RecordType;
import io.axual.ksml.exception.KSMLExecutionException;
import io.axual.ksml.avro.AvroNotation;
import io.axual.ksml.notation.Notation;
import io.axual.ksml.schema.DataSchema;
import io.axual.ksml.serde.UnknownTypeSerde;
import io.axual.ksml.util.DataUtil;
import io.axual.streams.proxy.axual.AxualSerdeConfig;

public class AxualAvroNotation implements Notation {
    private static final DataObjectMapper<Object> mapper = new AvroDataMapper();
    private final Map<String, Object> configs = new HashMap<>();

    public AxualAvroNotation(Map<String, Object> configs) {
        this.configs.putAll(configs);
        this.configs.put(AxualSerdeConfig.BACKING_KEY_SERDE_CONFIG, UnknownTypeSerde.class.getName());
        this.configs.put(AxualSerdeConfig.BACKING_VALUE_SERDE_CONFIG, UnknownTypeSerde.class.getName());
    }

    @Override
    public String name() {
        return AvroNotation.NOTATION_NAME;
    }

    public Serde<Object> getSerde(DataType type, DataSchema schema, boolean isKey) {
        if (type instanceof RecordType recordType) {
            if (schema == null && recordType.schema() != null) {
                schema = recordType.schema();
            }
            return new AvroSerde(configs, schema, isKey);
        }
        throw new KSMLExecutionException("Serde not found for data dataType " + type);
    }

    private static class AvroSerde implements Serde<Object> {
        private final Serde<GenericRecord> serde;

        public AvroSerde(Map<String, Object> configs, DataSchema schema, boolean isKey) {
            serde = new AxualAvroSerde(configs, schema, isKey);
        }

        private final Serializer<Object> wrapSerializer = new Serializer<>() {
            @Override
            public byte[] serialize(String topic, Object data) {
                var object = mapper.fromDataObject(DataUtil.asUserObject(data));
                if (object instanceof GenericRecord genericRecord) {
                    return serde.serializer().serialize(topic, genericRecord);
                }
                throw new KSMLExecutionException("Can not serialize using Avro: " + object.getClass().getSimpleName());
            }
        };

        private final Deserializer<Object> wrapDeserializer = new Deserializer<>() {
            @Override
            public Object deserialize(String topic, byte[] data) {
                GenericRecord object = serde.deserializer().deserialize(topic, data);
                return mapper.toDataObject(object);
            }
        };

        @Override
        public Serializer<Object> serializer() {
            return wrapSerializer;
        }

        @Override
        public Deserializer<Object> deserializer() {
            return wrapDeserializer;
        }
    }
}
