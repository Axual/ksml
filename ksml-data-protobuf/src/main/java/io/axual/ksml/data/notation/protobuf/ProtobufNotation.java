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
import io.axual.ksml.data.notation.base.BaseNotation;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.StructType;
import org.apache.kafka.common.serialization.Serde;

import java.util.Map;

public class ProtobufNotation extends BaseNotation {
    public static final String NOTATION_NAME = "protobuf";
    public static final DataType DEFAULT_TYPE = new StructType();
    private final ProtobufSerdeSupplier serdeSupplier;
    private final ProtobufDataObjectMapper protobufMapper;
    private final NativeDataObjectMapper nativeMapper;
    private final Map<String, String> serdeConfigs;

    public ProtobufNotation(ProtobufSerdeSupplier serdeSupplier, ProtobufSchemaParser schemaParser, ProtobufDataObjectMapper protobufMapper, NativeDataObjectMapper nativeMapper, Map<String, String> configs) {
        super(serdeSupplier.notationName(), serdeSupplier.vendorName(), ".proto", DEFAULT_TYPE, null, schemaParser);
        this.serdeSupplier = serdeSupplier;
        this.protobufMapper = protobufMapper;
        this.nativeMapper = nativeMapper;
        this.serdeConfigs = configs;
    }

    @Override
    public Serde<Object> serde(DataType type, boolean isKey) {
        return serdeSupplier.get(type, isKey);
    }
}
