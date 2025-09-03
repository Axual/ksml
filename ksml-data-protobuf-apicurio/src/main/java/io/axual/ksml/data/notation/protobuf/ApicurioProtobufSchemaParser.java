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


import io.axual.ksml.data.schema.DataSchema;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;

@Slf4j
public class ApicurioProtobufSchemaParser implements ProtobufSchemaParser {
    private static final ProtobufSchemaMapper MAPPER = new ProtobufSchemaMapper(new ApicurioProtobufDescriptorFileElementMapper());

    @Override
    public DataSchema parse(String contextName, String schemaName, String schemaString) {
        final var proto = new io.apicurio.registry.serde.protobuf.ProtobufSchemaParser<>().parseSchema(schemaString.getBytes(), Collections.emptyMap());
        return MAPPER.toDataSchema(schemaName, new ProtobufSchema(proto.getFileDescriptor(), proto.getProtoFileElement()));
    }
}
