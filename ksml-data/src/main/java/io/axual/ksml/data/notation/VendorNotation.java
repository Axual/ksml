package io.axual.ksml.data.notation;

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

import com.google.common.collect.ImmutableMap;
import io.axual.ksml.data.mapper.DataObjectMapper;
import io.axual.ksml.data.serde.DataObjectSerde;
import io.axual.ksml.data.type.DataType;
import lombok.Getter;
import org.apache.kafka.common.serialization.Serde;

import java.util.HashMap;
import java.util.Map;

public class VendorNotation extends BaseNotation {
    @Getter
    private final SerdeProvider serdeProvider;
    private final DataObjectMapper<Object> serdeMapper;
    private final DataObjectMapper<Object> nativeMapper;
    private final Map<String, Object> serdeConfigs;

    public VendorNotation(SerdeProvider serdeProvider, String filenameExtension, DataType defaultType, Notation.Converter converter, Notation.SchemaParser schemaParser, DataObjectMapper<Object> serdeMapper, DataObjectMapper<Object> nativeMapper, Map<String, String> serdeConfigs) {
        super(serdeProvider.name(), filenameExtension, defaultType, converter, schemaParser);
        this.serdeProvider = serdeProvider;
        this.serdeMapper = serdeMapper;
        this.nativeMapper = nativeMapper;
        this.serdeConfigs = serdeConfigs == null ? new HashMap<>() : ImmutableMap.copyOf(serdeConfigs);
    }

    @Override
    public Serde<Object> serde(DataType type, boolean isKey) {
        if (!defaultType().isAssignableFrom(type)) throw noSerdeFor(type);

        // Create the serdes only upon request to prevent error messages on missing SR url configs if AVRO is not used
        try (final var serde = serdeProvider.get(type, isKey)) {
            final var result = new DataObjectSerde(name(), serde.serializer(), serde.deserializer(), serdeMapper, nativeMapper);
            result.configure(serdeConfigs, isKey);
            return result;
        }
    }
}
