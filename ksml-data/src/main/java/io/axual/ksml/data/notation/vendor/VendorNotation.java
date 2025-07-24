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
import io.axual.ksml.data.notation.base.BaseNotation;
import io.axual.ksml.data.serde.DataObjectSerde;
import io.axual.ksml.data.type.DataType;
import lombok.Getter;
import org.apache.kafka.common.serialization.Serde;

import java.util.Map;

public abstract class VendorNotation extends BaseNotation {
    @Getter
    private final VendorSerdeSupplier serdeSupplier;
    private final DataObjectMapper<Object> serdeMapper;

    protected VendorNotation(VendorNotationContext context, String filenameExtension, DataType defaultType, Converter converter, SchemaParser schemaParser) {
        super(context, filenameExtension, defaultType, converter, schemaParser);
        this.serdeSupplier = context.serdeSupplier();
        this.serdeMapper = context.serdeMapper();
    }

    @Override
    public Serde<Object> serde(DataType type, boolean isKey) {
        if (!defaultType().isAssignableFrom(type)) throw noSerdeFor(type);

        // Create the serdes only upon request to prevent error messages on missing SR url configs if AVRO is not used
        try (final var serde = serdeSupplier.get(type, isKey)) {
            final var result = new DataObjectSerde(name(), serde.serializer(), serde.deserializer(), serdeMapper, context().nativeDataObjectMapper());
            result.configure(context().serdeConfigs(), isKey);
            return result;
        }
    }
}
