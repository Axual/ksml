package io.axual.ksml.data.notation.protobuf;

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

public class ProtobufNotation extends VendorNotation {
    public static final String NOTATION_NAME = "protobuf";
    public static final DataType DEFAULT_TYPE = new StructType();

    public ProtobufNotation(VendorNotationContext context, ProtobufSchemaParser schemaParser) {
        super(context, ".proto", DEFAULT_TYPE, null, schemaParser);
    }
}
