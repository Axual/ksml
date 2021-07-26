package io.axual.ksml;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;

import io.axual.ksml.avro.AvroDataMapper;
import io.axual.ksml.data.mapper.DataMapper;
import io.axual.ksml.util.DataUtil;
import io.axual.ksml.exception.KSMLExecutionException;
import io.axual.ksml.notation.AvroNotation;
import io.axual.ksml.notation.Notation;
import io.axual.ksml.serde.UnknownTypeSerde;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.RecordType;
import io.axual.streams.proxy.axual.AxualSerdeConfig;

public class AxualAvroNotation implements Notation {
    private static final DataMapper<Object> mapper = new AvroDataMapper();
    private final Map<String, Object> configs = new HashMap<>();

    public AxualAvroNotation(Map<String, Object> configs) {
        this.configs.putAll(configs);
        this.configs.put(AxualSerdeConfig.BACKING_KEY_SERDE_CONFIG, UnknownTypeSerde.class.getName());
        this.configs.put(AxualSerdeConfig.BACKING_VALUE_SERDE_CONFIG, UnknownTypeSerde.class.getName());
    }

    @Override
    public String name() {
        return AvroNotation.NAME;
    }

    public Serde<Object> getSerde(DataType type, boolean isKey) {
        if (type instanceof RecordType) {
            var result = new AvroSerde(configs, (RecordType) type, isKey);
            result.configure(configs, isKey);
            return result;
        }
        throw new KSMLExecutionException("Serde not found for data type " + type);
    }

    private class AvroSerde implements Serde<Object> {
        private final Serializer<GenericRecord> serializer;
        private final Deserializer<GenericRecord> deserializer;

        public AvroSerde(Map<String, Object> configs, RecordType type, boolean isKey) {
            AxualAvroSerde serde = new AxualAvroSerde(configs, type, isKey);
            serializer = serde.serializer();
            deserializer = serde.deserializer();
        }

        private final Serializer<Object> wrapSerializer = new Serializer<>() {
            @Override
            public byte[] serialize(String topic, Object data) {
                var object = mapper.fromDataObject(DataUtil.asData(data));
                if (object instanceof GenericRecord) {
                    return serializer.serialize(topic, (GenericRecord) object);
                }
                throw new KSMLExecutionException("Can not serialize using Avro: " + object.getClass().getSimpleName());
            }
        };

        private final Deserializer<Object> wrapDeserializer = new Deserializer<>() {
            @Override
            public Object deserialize(String topic, byte[] data) {
                GenericRecord object = deserializer.deserialize(topic, data);
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
