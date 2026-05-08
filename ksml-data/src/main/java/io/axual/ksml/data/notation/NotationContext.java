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

import io.axual.ksml.data.mapper.DataTypeDataSchemaMapper;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

/**
 * Context information for constructing and configuring a Notation.
 * Holds optional vendor identity, a NativeDataObjectMapper, and serde configuration properties.
 */
@Getter
public class NotationContext {
    private final NativeDataObjectMapper nativeDataObjectMapper;
    private final DataTypeDataSchemaMapper typeSchemaMapper;
    private final Map<String, String> serdeConfigs;

    /**
     * Creates a context for a notation with an optional vendor.
     */
    public NotationContext() {
        this(null);
    }

    /**
     * Creates a context with serde configuration properties.
     *
     * @param configs serde configuration map (nullable)
     */
    public NotationContext(Map<String, String> configs) {
        this(null, null, configs);
    }

    /**
     * Creates a context with a vendor and a custom NativeDataObjectMapper.
     *
     * @param nativeDataObjectMapper the custom native mapper
     * @param typeSchemaMapper       the custom type to schema mapper
     */
    public NotationContext(NativeDataObjectMapper nativeDataObjectMapper, DataTypeDataSchemaMapper typeSchemaMapper) {
        this(nativeDataObjectMapper, typeSchemaMapper, null);
    }

    /**
     * Creates a context with full control over vendor, native mapper, and serde configs.
     *
     * @param nativeDataObjectMapper the custom native mapper
     * @param typeSchemaMapper       the custom type to schema mapper
     * @param serdeConfigs           serde configuration map (nullable)
     */
    public NotationContext(NativeDataObjectMapper nativeDataObjectMapper, DataTypeDataSchemaMapper typeSchemaMapper, Map<String, String> serdeConfigs) {
        this.nativeDataObjectMapper = nativeDataObjectMapper != null ? nativeDataObjectMapper : new NativeDataObjectMapper();
        this.typeSchemaMapper = typeSchemaMapper != null ? typeSchemaMapper : new DataTypeDataSchemaMapper();
        this.serdeConfigs = serdeConfigs != null ? serdeConfigs : new HashMap<>();
    }
}
