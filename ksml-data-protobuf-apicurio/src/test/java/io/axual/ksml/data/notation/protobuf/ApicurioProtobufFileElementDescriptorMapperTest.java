package io.axual.ksml.data.notation.protobuf;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library - PROTOBUF Apicurio
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
import io.axual.ksml.data.notation.protobuf.apicurio.ApicurioProtobufFileElementDescriptorMapper;

import static org.assertj.core.api.Assertions.assertThat;

class ApicurioProtobufFileElementDescriptorMapperTest extends AbstractProtobufFileElementDescriptorMapperTest {

    private final ApicurioProtobufFileElementDescriptorMapper mapper = new ApicurioProtobufFileElementDescriptorMapper();

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
        assertThat(msgDescriptor.getOneofs()).hasSize(1);
        assertThat(msgDescriptor.getOneofs().getFirst().getName()).isEqualTo("payload");
        assertThat(msgDescriptor.getOneofs().getFirst().getFields()).hasSize(2);
    }
}
