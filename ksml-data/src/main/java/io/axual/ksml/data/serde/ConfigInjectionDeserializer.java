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
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class ConfigInjectionDeserializer implements Deserializer<Object> {
    private final Deserializer<Object> delegate;

    public ConfigInjectionDeserializer(Deserializer<Object> delegate) {
        this.delegate = delegate;
    }

    protected Map<String, ?> modifyConfigs(Map<String, ?> configs, boolean isKey) {
        return configs;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        delegate.configure(modifyConfigs(configs, isKey), isKey);
    }

    @Override
    public Object deserialize(String topic, Headers headers, byte[] data) {
        return delegate.deserialize(topic, headers, data);
    }

    @Override
    public Object deserialize(String topic, Headers headers, ByteBuffer data) {
        return delegate.deserialize(topic, headers, data);
    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        return delegate.deserialize(topic, data);
    }

    @Override
    public void close() {
        delegate.close();
    }
}
