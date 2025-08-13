package io.axual.ksml.data.notation.avro;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2023 Axual B.V.
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

import io.axual.ksml.data.notation.vendor.VendorNotation;
import io.axual.ksml.data.notation.vendor.VendorNotationContext;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.StructType;

/**
 * KSML Notation implementation for Avro.
 *
 * <p>Provides wiring for vendor-backed Avro serdes via VendorNotation. The default KSML type
 * for Avro is StructType, and schema parsing is delegated to AvroSchemaParser for .avsc files.</p>
 */
public class AvroNotation extends VendorNotation {
    public static final String NOTATION_NAME = "avro";
    public static final DataType DEFAULT_TYPE = new StructType();
    private static final AvroSchemaParser AVRO_SCHEMA_PARSER = new AvroSchemaParser();

    /**
     * Construct an AvroNotation with the provided vendor context.
     *
     * @param context the vendor notation context providing serde supplier, native mapper, and configs
     */
    public AvroNotation(VendorNotationContext context) {
        super(context, ".avsc", DEFAULT_TYPE, null, AVRO_SCHEMA_PARSER);
    }
}
