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

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class ConfigInjectionSerializerTest {
    private static final String TOPIC = "topic";

    @Test
    @DisplayName("configure injects extra config entries before delegating; serialize delegates")
    void configIsInjectedAndSerializeDelegates() {
        var seenConfig = new AtomicReference<Map<String, ?>>();
        var delegate = new Serializer<>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
                seenConfig.set(configs);
            }

            @Override
            public byte[] serialize(String topic, Object data) {
                return data == null ? null : data.toString().getBytes();
            }

            @Override
            public byte[] serialize(String topic, org.apache.kafka.common.header.Headers headers, Object data) {
                return data == null ? null : data.toString().getBytes();
            }
        };

        var configInjecting = new ConfigInjectionSerializer(delegate) {
            @Override
            protected Map<String, Object> modifyConfigs(Map<String, Object> configs, boolean isKey) {
                configs.put("injected", "yes");
                configs.put("isKey", isKey);
                return configs;
            }
        };

        var inputConfig = Map.of("a", "b");
        configInjecting.configure(inputConfig, true);
        var seen = seenConfig.get();
        assertThat(seen.get("a")).isEqualTo("b");
        assertThat(seen.get("injected")).isEqualTo("yes");
        assertThat(seen.get("isKey")).isEqualTo(true);

        var headers = new RecordHeaders();
        assertThat(configInjecting.serialize(TOPIC, "x")).isEqualTo("x".getBytes());
        assertThat(configInjecting.serialize(TOPIC, headers, "y")).isEqualTo("y".getBytes());
    }
}
