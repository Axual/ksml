package io.axual.ksml.serde;

import io.axual.ksml.data.mapper.DataObjectMapper;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.exception.KSMLExecutionException;
import io.axual.ksml.execution.FatalError;
import io.axual.ksml.util.DataUtil;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class StringSerde implements Serde<Object> {
    private final DataType expectedType;
    private final DataObjectMapper<String> mapper;

    public StringSerde(DataObjectMapper<String> mapper, DataType expectedType) {
        this.expectedType = expectedType;
        this.mapper = mapper;
    }

    private final StringSerializer serializer = new StringSerializer();
    private final StringDeserializer deserializer = new StringDeserializer();

    @Override
    public Serializer<Object> serializer() {
        return (topic, data) -> {
            final var dataObject = DataUtil.asDataObject(data);
            if (!expectedType.isAssignableFrom(dataObject)) {
                throw new KSMLExecutionException("Incorrect type passed in: expected=" + expectedType + ", got " + dataObject.type());
            }
            var str = mapper.fromDataObject(DataUtil.asDataObject(data));
            return serializer.serialize(topic, str);
        };
    }

    @Override
    public Deserializer<Object> deserializer() {
        return (topic, data) -> {
            final var str = deserializer.deserialize(topic, data);
            final var dataObject = mapper.toDataObject(expectedType, str);
            if (dataObject != null && !expectedType.isAssignableFrom(dataObject)) {
                throw FatalError.executionError("Wrong type retrieved from state store: expected " + expectedType + ", got " + dataObject.type());
            }
            return dataObject;
        };
    }
}
