package io.axual.ksml.client.serde;

import io.axual.ksml.client.generic.ResolvingClientConfig;
import io.axual.ksml.client.resolving.TopicResolver;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class ResolvingSerializer<T> implements Serializer<T> {
    private final Serializer<T> backingSerializer;
    private TopicResolver topicResolver;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        backingSerializer.configure(configs, isKey);
        this.topicResolver = new ResolvingClientConfig(configs).getTopicResolver();
    }

    public ResolvingSerializer(Serializer<T> backingSerializer, Map<String, ?> configs) {
        this.backingSerializer = backingSerializer;
        this.topicResolver = new ResolvingClientConfig(configs).getTopicResolver();
    }

    @Override
    public byte[] serialize(String topic, T object) {
        return backingSerializer.serialize(topicResolver != null ? topicResolver.resolve(topic) : topic, object);
    }

    @Override
    public byte[] serialize(String topic, Headers headers, T object) {
        return backingSerializer.serialize(topicResolver != null ? topicResolver.resolve(topic) : topic, headers, object);
    }

    @Override
    public void close() {
        backingSerializer.close();
    }
}
