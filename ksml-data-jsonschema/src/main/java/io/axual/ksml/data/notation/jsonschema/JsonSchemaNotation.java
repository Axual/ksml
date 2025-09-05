package io.axual.ksml.data.notation.jsonschema;

/*-
 * ========================LICENSE_START=================================
 * KSML
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

import io.axual.ksml.data.notation.vendor.VendorNotation;
import io.axual.ksml.data.notation.json.JsonDataObjectConverter;
import io.axual.ksml.data.notation.json.JsonSchemaLoader;
import io.axual.ksml.data.notation.vendor.VendorNotationContext;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.data.type.StructType;
import io.axual.ksml.data.type.UnionType;

/**
 * JSON Schema notation implementation for KSML using vendor-backed serdes.
 *
 * <p>Responsibilities:</p>
 * <ul>
 *   <li>Expose the notation name {@link #NOTATION_NAME} ("jsonschema").</li>
 *   <li>Define the default data type as a union of {@link StructType} and {@link ListType}
 *       to support both JSON objects and arrays.</li>
 *   <li>Wire a converter ({@link JsonDataObjectConverter}) and schema parser ({@link JsonSchemaLoader}).</li>
 *   <li>Create vendor-backed serdes via {@link VendorNotation} when types are assignable from the default type.</li>
 * </ul>
 */
public class JsonSchemaNotation extends VendorNotation {
    /** Canonical notation name for JSON Schema. */
    public static final String NOTATION_NAME = "jsonschema";
    /** Default supported data type: union of Struct and List. */
    public static final DataType DEFAULT_TYPE = new UnionType(
            new UnionType.MemberType(new StructType()),
            new UnionType.MemberType(new ListType()));

    /**
     * Creates a JsonSchemaNotation with the provided vendor context.
     * The filename extension is ".json" to match JSON Schema files.
     */
    public JsonSchemaNotation(VendorNotationContext context) {
        // Wire the VendorNotation with JSON-specific converter and schema loader.
        super(context,  ".json", DEFAULT_TYPE, new JsonDataObjectConverter(), new JsonSchemaLoader());
    }
}
