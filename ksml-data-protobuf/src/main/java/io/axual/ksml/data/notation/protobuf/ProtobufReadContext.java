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

import com.squareup.wire.schema.internal.parser.TypeElement;
import io.apicurio.registry.utils.protobuf.schema.ProtobufSchema;

import java.util.List;

public class ProtobufReadContext {
    private final ProtobufSchema schema;
    private final String namespace;

    public ProtobufReadContext(ProtobufSchema schema) {
        this.schema = schema;
        this.namespace = schema.getFileDescriptor().getPackage();
    }

    public String namespace() {
        return schema.getProtoFileElement().getPackageName();
    }

    public record FindResult(String namespace, TypeElement type) {
    }

    public FindResult type(String name) {
        final var fileElement = schema.getProtoFileElement();
        final var descriptor = findType(fileElement.getPackageName(), fileElement.getTypes(), name);
        if (descriptor != null) return descriptor;
        final var enm = ProtobufUtil.findInList(fileElement.getTypes(), TypeElement::getName, namespace + "." + name);
        if (enm != null) return new FindResult(namespace, enm);
        return null;
    }

    private FindResult findType(String namespace, List<TypeElement> types, String name) {
        for (final var type : types) {
            if (type.getName().equals(name)) return new FindResult(namespace, type);
            final var subMsg = findType(namespace + "." + type.getName(), type.getNestedTypes(), name);
            if (subMsg != null) return subMsg;
        }
        return null;
    }
}
