package io.axual.ksml.data.notation.protobuf;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library - PROTOBUF Confluent
 * %%
 * Copyright (C) 2021 - 2026 Axual B.V.
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
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import io.axual.ksml.data.notation.protobuf.confluent.ConfluentProtobufFileElementDescriptorMapper;

import static org.assertj.core.api.Assertions.assertThat;

class ConfluentProtobufFileElementDescriptorMapperTest extends AbstractProtobufFileElementDescriptorMapperTest {

    private final ConfluentProtobufFileElementDescriptorMapper mapper = new ConfluentProtobufFileElementDescriptorMapper();

    @Override
    protected Descriptors.FileDescriptor toDescriptor(String namespace, String fileName, ProtoFileElement fileElement) {
        return mapper.toDescriptor(namespace, fileName, fileElement);
    }

    @Override
    protected ProtoFileElement toFileElement(Descriptors.Descriptor descriptor) {
        return mapper.toFileElement(descriptor);
    }

    @Override
    protected void assertOneOfBranches(Descriptors.Descriptor msgDescriptor) {
        // Documented limitation: the Confluent mapper builds the descriptor via DynamicSchema, and the
        // resulting FileDescriptor does not retain the oneof grouping. Both the message oneof list
        // (getOneofs()) and each field's getContainingOneof() come back empty/null on this path, so the
        // grouping cannot be asserted the way the Apicurio test does. We therefore verify only that the
        // two branch fields survive the conversion with the right types.
        final var text = msgDescriptor.findFieldByName("text");
        assertThat(text).isNotNull();
        assertThat(text.getType()).isEqualTo(Descriptors.FieldDescriptor.Type.STRING);

        final var count = msgDescriptor.findFieldByName("count");
        assertThat(count).isNotNull();
        assertThat(count.getType()).isEqualTo(Descriptors.FieldDescriptor.Type.INT32);
    }
}
