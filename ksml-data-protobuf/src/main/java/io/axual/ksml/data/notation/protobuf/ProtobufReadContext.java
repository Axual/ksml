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
import io.apicurio.registry.utils.protobuf.schema.ProtobufSchema;

import java.util.List;

public class ProtobufReadContext {
    private final ProtobufSchema schema;

    public ProtobufReadContext(ProtobufSchema schema) {
        this.schema = schema;
    }

    public String namespace() {
        return schema.getProtoFileElement().getPackageName();
    }

    public Descriptors.GenericDescriptor type(String name) {
        final var fd = schema.getFileDescriptor();
        final var descriptor = findType(fd.getMessageTypes(), name);
        if (descriptor != null) return descriptor;
        return ProtobufUtil.findInList(fd.getEnumTypes(), Descriptors.EnumDescriptor::getName, name);
    }

    private Descriptors.GenericDescriptor findType(List<Descriptors.Descriptor> descriptors, String name) {
        for (final var descriptor : descriptors) {
            if (descriptor.getName().equals(name)) return descriptor;
            final var subMsg = findType(descriptor.getNestedTypes(), name);
            if (subMsg != null) return subMsg;
            final var subEnum = descriptor.findEnumTypeByName(name);
            if (subEnum != null) return subEnum;
        }
        return null;
    }
}
