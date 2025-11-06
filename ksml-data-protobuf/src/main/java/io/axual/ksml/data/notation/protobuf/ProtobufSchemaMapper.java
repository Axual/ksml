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

import io.axual.ksml.data.exception.SchemaException;
import io.axual.ksml.data.mapper.DataSchemaMapper;
import io.axual.ksml.data.mapper.DataTypeDataSchemaMapper;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.NamedSchema;
import io.axual.ksml.data.schema.StructSchema;

public class ProtobufSchemaMapper implements DataSchemaMapper<ProtobufSchema> {
    private final ProtobufFileElementDescriptorMapper elementDescriptorMapper;
    private final ProtobufFileElementSchemaMapper elementSchemaMapper;

    public ProtobufSchemaMapper(ProtobufFileElementDescriptorMapper elementDescriptorMapper) {
        this(elementDescriptorMapper, new NativeDataObjectMapper(), new DataTypeDataSchemaMapper());
    }

    public ProtobufSchemaMapper(ProtobufFileElementDescriptorMapper elementDescriptorMapper, NativeDataObjectMapper nativeMapper, DataTypeDataSchemaMapper typeSchemaMapper) {
        this.elementDescriptorMapper = elementDescriptorMapper;
        this.elementSchemaMapper = new ProtobufFileElementSchemaMapper(nativeMapper, typeSchemaMapper);
    }

    @Override
    public DataSchema toDataSchema(String namespace, String name, ProtobufSchema schema) {
        return elementSchemaMapper.toDataSchema(namespace, name, schema.protoFileElement());
    }

    @Override
    public ProtobufSchema fromDataSchema(DataSchema schema) {
        final var name = schema instanceof NamedSchema namedSchema ? " '" + namedSchema.name() + "'" : null;
        if (schema instanceof StructSchema structSchema) {
            final var fileElement = elementSchemaMapper.fromDataSchema(structSchema);
            final var descriptor = elementDescriptorMapper.toDescriptor(structSchema.namespace(), structSchema.name(), fileElement);
            return new ProtobufSchema(descriptor, fileElement);
        }
        throw new SchemaException("Can not convert " + schema.type() + " into dynamic PROTOBUF schema" + name);
    }
}
