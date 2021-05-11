package io.axual.ksml;

/*-
 * ========================LICENSE_START=================================
 * KSML for Axual
 * %%
 * Copyright (C) 2021 Axual B.V.
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


import org.apache.kafka.common.serialization.Serde;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import io.axual.ksml.generator.DefaultSerdeGenerator;
import io.axual.ksml.generator.SerdeGenerator;
import io.axual.ksml.type.AvroType;
import io.axual.ksml.type.DataType;

public class AxualSerdeGenerator implements SerdeGenerator {
    private final Map<String, Object> configs;
    private final DefaultSerdeGenerator defaultSerdeGenerator;

    public AxualSerdeGenerator(Map<String, Object> configs) {
        this.configs = configs;
        defaultSerdeGenerator = new DefaultSerdeGenerator(configs);
    }

    public AxualSerdeGenerator(Properties properties) {
        configs = new HashMap<>();
        properties.forEach((k, v) -> configs.put((String) k, v));
        defaultSerdeGenerator = new DefaultSerdeGenerator(configs);
    }

    public Serde<Object> getSerdeForType(final DataType type, boolean isKey) {
        if (type instanceof AvroType) {
            return (Serde) new AvroSerde(configs, (AvroType) type, isKey);
        }
        return defaultSerdeGenerator.getSerdeForType(type, isKey);
    }
}
