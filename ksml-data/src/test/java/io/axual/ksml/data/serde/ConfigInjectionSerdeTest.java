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

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;

class ConfigInjectionSerdeTest {
    @ParameterizedTest
    @DisplayName("ConfigInjectionSerde routes modified configs to both underlying serializer and deserializer")
    @ValueSource(booleans = {true, false})
    void serdeInjectsConfigsForBothSides(boolean isKey) {
        final var captureNonModifiedSerializer = new ConfigCaptureSerializer();
        final var captureNonModifiedDeserializer = new ConfigCaptureDeserializer();
        final var captureNonModifiedSerde = new ConfigInjectionSerde(Serdes.serdeFrom(captureNonModifiedSerializer, captureNonModifiedDeserializer));

        final var captureModifiedSerializer = new ConfigCaptureSerializer();
        final var captureModifiedDeserializer = new ConfigCaptureDeserializer();
        final var captureModifiedSerde = new ConfigInjectionSerde(Serdes.serdeFrom(captureModifiedSerializer, captureModifiedDeserializer)) {
            @Override
            protected Map<String, ?> modifyConfigs(final Map<String, ?> configs, final boolean isKey) {
                var map = new HashMap<String, Object>(configs);
                map.put("isKey", Boolean.toString(isKey));
                return map;
            }
        };

        final var config = Map.<String, Object>of("firstKey", "firstValue", "secondKey", "secondValue");
        final var expectedNonModifiedConfig = new HashMap<>(config);
        final var expectedModifiedConfig = new HashMap<>(config);
        expectedModifiedConfig.put("isKey", Boolean.toString(isKey));

        captureModifiedSerde.configure(config, isKey);
        captureNonModifiedSerde.configure(config, isKey);

        final var softly = new SoftAssertions();

        softly.assertThat(captureNonModifiedSerializer.configs)
                .as("Verify serializer config injection with base ConfigInjectionSerde")
                .isNotNull()
                .containsExactlyEntriesOf(expectedNonModifiedConfig);
        softly.assertThat(captureNonModifiedDeserializer.configs)
                .as("Verify deserializer config injection with base ConfigInjectionSerde")
                .isNotNull()
                .containsExactlyEntriesOf(expectedNonModifiedConfig);
        softly.assertThat(captureNonModifiedSerializer.isKey)
                .as("Verify serializer key setting with base ConfigInjectionSerde")
                .isNotNull()
                .isEqualTo(isKey);
        softly.assertThat(captureNonModifiedDeserializer.isKey)
                .as("Verify deserializer key setting with base ConfigInjectionSerde")
                .isNotNull()
                .isEqualTo(isKey);

        softly.assertThat(captureModifiedSerializer.configs)
                .as("Verify serializer config injection with isKey injecting ConfigInjectionSerde")
                .isNotNull()
                .containsExactlyEntriesOf(expectedModifiedConfig);
        softly.assertThat(captureModifiedSerializer.configs)
                .as("Verify deserializer config injection with isKey injecting ConfigInjectionSerde")
                .isNotNull()
                .containsExactlyEntriesOf(expectedModifiedConfig);
        softly.assertThat(captureModifiedSerializer.isKey)
                .as("Verify serializer key setting with isKey injecting ConfigInjectionSerde")
                .isNotNull()
                .isEqualTo(isKey);
        softly.assertThat(captureModifiedSerializer.isKey)
                .as("Verify deserializer key setting with isKey injecting ConfigInjectionSerde")
                .isNotNull()
                .isEqualTo(isKey);

        softly.assertAll();


    }

    static class ConfigCaptureSerializer implements Serializer<Object> {
        Map<String, Object> configs = null;
        Boolean isKey = null;

        @Override
        public void configure(final Map<String, ?> configs, final boolean isKey) {
            this.configs = new HashMap<>(configs);
            this.isKey = isKey;
        }

        @Override
        public byte[] serialize(final String topic, final Object data) {
            return new byte[0];
        }
    }

    static class ConfigCaptureDeserializer implements Deserializer<Object> {
        Map<String, Object> configs = null;
        Boolean isKey = null;


        @Override
        public void configure(final Map<String, ?> configs, final boolean isKey) {
            this.configs = new HashMap<>(configs);
            this.isKey = isKey;
        }

        @Override
        public Object deserialize(final String topic, final byte[] data) {
            return null;
        }
    }
}
