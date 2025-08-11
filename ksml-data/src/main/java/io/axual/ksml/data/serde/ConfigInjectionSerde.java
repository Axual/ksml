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

import lombok.Getter;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Serde wrapper that injects or mutates configuration for both its serializer and
 * deserializer by delegating to ConfigInjectionSerializer and ConfigInjectionDeserializer.
 */
@Getter
public class ConfigInjectionSerde implements Serde<Object> {
    private final Serializer<Object> serializer;
    private final Deserializer<Object> deserializer;

    /**
     * Creates a ConfigInjectionSerde using a delegate Serde.
     *
     * @param delegate the underlying Serde to wrap
     */
    public ConfigInjectionSerde(Serde<Object> delegate) {
        this(delegate.serializer(), delegate.deserializer());
    }

    /**
     * Creates a ConfigInjectionSerde from separate serializer and deserializer delegates.
     *
     * @param serializer   the underlying Serializer to wrap
     * @param deserializer the underlying Deserializer to wrap
     */
    public ConfigInjectionSerde(Serializer<Object> serializer, Deserializer<Object> deserializer) {
        this.serializer = new ConfigInjectionSerializer(serializer) {
            @Override
            protected Map<String, ?> modifyConfigs(Map<String, ?> configs, boolean isKey) {
                return ConfigInjectionSerde.this.modifyConfigs(configs, isKey);
            }
        };
        this.deserializer = new ConfigInjectionDeserializer(deserializer) {
            @Override
            protected Map<String, ?> modifyConfigs(Map<String, ?> configs, boolean isKey) {
                return ConfigInjectionSerde.this.modifyConfigs(configs, isKey);
            }
        };
    }

    protected Map<String, ?> modifyConfigs(Map<String, ?> configs, boolean isKey) {
        return configs;
    }
}
