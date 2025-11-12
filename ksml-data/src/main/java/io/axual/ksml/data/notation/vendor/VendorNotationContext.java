package io.axual.ksml.data.notation.vendor;

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

import io.axual.ksml.data.mapper.DataObjectMapper;
import io.axual.ksml.data.notation.NotationContext;
import lombok.Getter;

/**
 * Extension of NotationContext that includes vendor-specific serde supplier and mapping.
 */
@Getter
public class VendorNotationContext extends NotationContext {
    private final VendorSerdeSupplier serdeSupplier;
    private final DataObjectMapper<Object> serdeMapper;

    /**
     * Creates a vendor notation context from an existing base context together with vendor serde components.
     *
     * @param context       the base notation context
     * @param serdeSupplier the vendor-specific serde supplier
     * @param serdeMapper   the DataObject mapper used with the vendor serdes
     */
    public VendorNotationContext(NotationContext context, VendorSerdeSupplier serdeSupplier, DataObjectMapper<Object> serdeMapper) {
        super(context.notationName(), context.vendorName(), context.nativeDataObjectMapper(), context.typeSchemaMapper(), context.serdeConfigs());
        this.serdeSupplier = serdeSupplier;
        this.serdeMapper = serdeMapper;
    }
}
