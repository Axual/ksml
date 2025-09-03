package io.axual.ksml.data.serde;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library
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

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Serializer wrapper that allows subclasses to inject or mutate configuration
 * properties before delegating to the underlying Serializer.
 */
public class ConfigInjectionSerializer implements Serializer<Object> {
    private final Serializer<Object> delegate;

    /**
     * Creates a new ConfigInjectionSerializer.
     *
     * @param delegate the underlying Serializer to delegate to
     */
    public ConfigInjectionSerializer(Serializer<Object> delegate) {
        this.delegate = delegate;
    }

    protected Map<String, ?> modifyConfigs(Map<String, ?> configs, boolean isKey) {
        return configs;
    }

    /**
     * Configures the delegate serializer after allowing subclasses to modify the provided configs.
     *
     * @param configs the original configuration map
     * @param isKey   whether this serializer is used for record keys
     */
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        delegate.configure(modifyConfigs(configs, isKey), isKey);
    }

    /**
     * Delegates serialization without headers to the underlying serializer.
     *
     * @param topic the topic name
     * @param data  the value to serialize, may be null
     * @return the serialized bytes
     */
    @Override
    public byte[] serialize(String topic, Object data) {
        return delegate.serialize(topic, data);
    }

    /**
     * Delegates serialization with headers to the underlying serializer.
     *
     * @param topic   the topic name
     * @param headers the record headers to include
     * @param data    the value to serialize, may be null
     * @return the serialized bytes
     */
    @Override
    public byte[] serialize(String topic, Headers headers, Object data) {
        return delegate.serialize(topic, headers, data);
    }

    /**
     * Closes the underlying delegate serializer.
     */
    @Override
    public void close() {
        delegate.close();
    }
}
