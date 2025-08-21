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
    private final String notationName;
    private final String vendorName;
    private final NativeDataObjectMapper nativeDataObjectMapper;
    private final Map<String, String> serdeConfigs;

    /**
     * Creates a context for a non-vendor specific notation name.
     *
     * @param notationName the notation name
     */
    public NotationContext(String notationName) {
        this(notationName, (String) null);
    }

    /**
     * Creates a context for a notation with an optional vendor.
     *
     * @param notationName the notation name
     * @param vendorName the vendor name (nullable)
     */
    public NotationContext(String notationName, String vendorName) {
        this(notationName, vendorName, (Map<String, String>) null);
    }

    /**
     * Creates a context with serde configuration properties.
     *
     * @param notationName the notation name
     * @param vendorName the vendor name (nullable)
     * @param configs serde configuration map (nullable)
     */
    public NotationContext(String notationName, String vendorName, Map<String, String> configs) {
        this(notationName, vendorName, new NativeDataObjectMapper(), configs);
    }

    /**
     * Creates a context with a custom NativeDataObjectMapper.
     *
     * @param notationName the notation name
     * @param nativeDataObjectMapper the custom native mapper
     */
    public NotationContext(String notationName, NativeDataObjectMapper nativeDataObjectMapper) {
        this(notationName, null, nativeDataObjectMapper, null);
    }

    /**
     * Creates a context with a vendor and a custom NativeDataObjectMapper.
     *
     * @param notationName the notation name
     * @param vendorName the vendor name (nullable)
     * @param nativeDataObjectMapper the custom native mapper
     */
    public NotationContext(String notationName, String vendorName, NativeDataObjectMapper nativeDataObjectMapper) {
        this(notationName, vendorName, nativeDataObjectMapper, null);
    }

    /**
     * Creates a context with full control over vendor, native mapper and serde configs.
     *
     * @param notationName the notation name
     * @param vendorName the vendor name (nullable)
     * @param nativeDataObjectMapper the custom native mapper
     * @param serdeConfigs serde configuration map (nullable)
     */
    public NotationContext(String notationName, String vendorName, NativeDataObjectMapper nativeDataObjectMapper, Map<String, String> serdeConfigs) {
        this.notationName = notationName;
        this.vendorName = vendorName;
        this.nativeDataObjectMapper = nativeDataObjectMapper;
        this.serdeConfigs = serdeConfigs != null ? serdeConfigs : new HashMap<>();
    }

    /**
     * Builds the display/identifier name for the notation context, including an optional
     * vendor prefix separated by an underscore.
     *
     * @return the combined name, eg. "vendor_notation" or just "notation"
     */
    public String name() {
        return (vendorName() != null && !vendorName().isEmpty() ? vendorName() + "_" : "") + notationName();
    }
}
