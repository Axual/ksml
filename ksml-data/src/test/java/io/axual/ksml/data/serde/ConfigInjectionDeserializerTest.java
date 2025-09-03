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
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class ConfigInjectionDeserializerTest {
    private static final String TOPIC = "topic";

    @Test
    @DisplayName("configure injects extra config entries before delegating; deserialize delegates for all overloads")
    void configIsInjectedAndDeserializeDelegates() {
        var seenConfig = new AtomicReference<Map<String, ?>>();
        var delegate = new Deserializer<>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
                seenConfig.set(configs);
            }

            @Override
            public Object deserialize(String topic, byte[] data) {
                return data == null ? null : new String(data);
            }

            @Override
            public Object deserialize(String topic, org.apache.kafka.common.header.Headers headers, byte[] data) {
                return data == null ? null : new String(data);
            }

            @Override
            public Object deserialize(String topic, org.apache.kafka.common.header.Headers headers, ByteBuffer data) {
                var arr = new byte[data.remaining()];
                data.get(arr);
                return new String(arr);
            }
        };

        var configInjecting = new ConfigInjectionDeserializer(delegate) {
            @Override
            protected Map<String, ?> modifyConfigs(Map<String, ?> configs, boolean isKey) {
                var copy = new HashMap<String, Object>();
                copy.putAll(configs);
                copy.put("injected", "yes");
                copy.put("isKey", isKey);
                return copy;
            }
        };

        var inputConfig = Map.of("a", "b");
        configInjecting.configure(inputConfig, true);
        var seen = seenConfig.get();
        assertThat(seen.get("a")).isEqualTo("b");
        assertThat(seen.get("injected")).isEqualTo("yes");
        assertThat(seen.get("isKey")).isEqualTo(true);

        var headers = new RecordHeaders();
        assertThat(configInjecting.deserialize(TOPIC, "x".getBytes())).isEqualTo("x");
        assertThat(configInjecting.deserialize(TOPIC, headers, "y".getBytes())).isEqualTo("y");
        assertThat(configInjecting.deserialize(TOPIC, headers, ByteBuffer.wrap("z".getBytes()))).isEqualTo("z");
    }
}
