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

import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.notation.VendorNotation;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.StructType;

import java.util.Map;
import java.util.ServiceLoader;

public class ProtobufNotation extends VendorNotation {
    public static final String NOTATION_NAME = "protobuf";
    public static final DataType DEFAULT_TYPE = new StructType();

    public ProtobufNotation(ProtobufSerdeProvider serdeProvider, ProtobufSchemaParser schemaParser, ProtobufDataObjectMapper protobufMapper, NativeDataObjectMapper nativeMapper, Map<String, String> configs) {
        super(serdeProvider, ".proto", DEFAULT_TYPE, null, schemaParser, protobufMapper, nativeMapper, configs);
    }

    public static ServiceLoader<ProtobufSerdeProvider> getSerdeProviders() {
        return ServiceLoader.load(ProtobufSerdeProvider.class);
    }
}
