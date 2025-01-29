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

import io.apicurio.registry.utils.protobuf.schema.ProtobufSchema;
import io.axual.ksml.data.exception.SchemaException;
import io.axual.ksml.data.mapper.DataSchemaMapper;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.NamedSchema;
import io.axual.ksml.data.schema.StructSchema;

public class ProtobufSchemaMapper implements DataSchemaMapper<ProtobufSchema> {
    private static final ProtobufDescriptorFileElementMapper DESCRIPTOR_ELEMENT_MAPPER = new ProtobufDescriptorFileElementMapper();
    private static final ProtobufFileElementSchemaMapper ELEMENT_SCHEMA_MAPPER = new ProtobufFileElementSchemaMapper();

    @Override
    public DataSchema toDataSchema(String namespace, String name, ProtobufSchema schema) {
        return ELEMENT_SCHEMA_MAPPER.toDataSchema(namespace, name, schema.getProtoFileElement());
    }

    @Override
    public ProtobufSchema fromDataSchema(DataSchema schema) {
        final var name = schema instanceof NamedSchema namedSchema ? " '" + namedSchema.name() + "'" : null;
        if (schema instanceof StructSchema structSchema) {
            final var fileElement = ELEMENT_SCHEMA_MAPPER.fromDataSchema(structSchema);
            final var descriptor = DESCRIPTOR_ELEMENT_MAPPER.toDescriptor(structSchema.namespace(), structSchema.name(), fileElement);
            return new ProtobufSchema(descriptor, fileElement);
        }
        throw new SchemaException("Can not convert " + schema.type() + " into dynamic PROTOBUF schema" + name);
    }
}
