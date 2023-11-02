package io.axual.ksml.client.serde;

import io.axual.ksml.client.generic.ResolvingClientConfig;
import io.axual.ksml.client.resolving.TopicResolver;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class ResolvingDeserializer<T> implements Deserializer<T> {
    private final Deserializer<T> backingDeserializer;
    private TopicResolver topicResolver;

    public ResolvingDeserializer(Deserializer<T> backingDeserializer, Map<String, ?> configs) {
        this.backingDeserializer = backingDeserializer;
        this.topicResolver = new ResolvingClientConfig(configs).getTopicResolver();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        backingDeserializer.configure(configs, isKey);
        this.topicResolver = new ResolvingClientConfig(configs).getTopicResolver();
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        return backingDeserializer.deserialize(topicResolver != null ? topicResolver.resolve(topic) : topic, data);
    }

    @Override
    public T deserialize(String topic, Headers headers, byte[] data) {
        return backingDeserializer.deserialize(topicResolver != null ? topicResolver.resolve(topic) : topic, headers, data);
    }

    @Override
    public T deserialize(String topic, Headers headers, ByteBuffer data) {
        return backingDeserializer.deserialize(topicResolver != null ? topicResolver.resolve(topic) : topic, headers, data);
    }

    @Override
    public void close() {
        backingDeserializer.close();
    }
}
