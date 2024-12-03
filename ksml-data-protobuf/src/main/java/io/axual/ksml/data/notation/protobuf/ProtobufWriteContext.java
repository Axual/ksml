package io.axual.ksml.data.notation.protobuf;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library - PROTOBUF
 * %%
 * Copyright (C) 2021 - 2024 Axual B.V.
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

import com.google.protobuf.Descriptors;
import com.squareup.wire.Syntax;
import io.apicurio.registry.serde.protobuf.ProtobufSchemaParser;
import io.apicurio.registry.utils.protobuf.schema.DynamicSchema;
import io.apicurio.registry.utils.protobuf.schema.EnumDefinition;
import io.apicurio.registry.utils.protobuf.schema.MessageDefinition;
import io.apicurio.registry.utils.protobuf.schema.ProtobufSchema;
import io.axual.ksml.data.exception.SchemaException;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

public class ProtobufWriteContext {
    private static final ProtobufSchemaMapper MAPPER = new ProtobufSchemaMapper();
    private static final Syntax DEFAULT_SYNTAX = Syntax.PROTO_3;
    private static final Descriptors.FileDescriptor[] NO_DEPENDENCIES = new Descriptors.FileDescriptor[0];
    @Getter
    private final String namespace;
    private final String name;
    private final Map<String, MessageDefinition> messages = new HashMap<>();
    private final Map<String, EnumDefinition> enums = new HashMap<>();

    public ProtobufWriteContext(String namespace, String name) {
        this.namespace = namespace;
        this.name = name;
    }

    public void addMessage(String name, MessageDefinition messageDef) {
        messages.put(name, messageDef);
    }

    public void addEnum(String name, EnumDefinition enumDef) {
        enums.put(name, enumDef);
    }

    public ProtobufSchema toProtobufSchema() {
        final var result = DynamicSchema.newBuilder();
        result.setSyntax(DEFAULT_SYNTAX.toString());
        result.setPackage(namespace);
        result.setName(name);
        messages.forEach((name, msg) -> result.addMessageDefinition(msg));
        enums.forEach((name, enm) -> result.addEnumDefinition(enm));
        try {
            final var ds = result.build();
            final var fd = Descriptors.FileDescriptor.buildFrom(ds.getFileDescriptorProto(), NO_DEPENDENCIES);
            final var fe = new ProtobufSchemaParser<>().toProtoFileElement(fd);
            return new ProtobufSchema(fd, fe);
        } catch (Descriptors.DescriptorValidationException e) {
            throw new SchemaException("Could not generate dynamic PROTOBUF schema" + name + ": " + e.getMessage(), e);
        }
    }
}
