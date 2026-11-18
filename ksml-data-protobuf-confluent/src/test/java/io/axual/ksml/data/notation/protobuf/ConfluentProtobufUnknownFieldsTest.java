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
import io.axual.ksml.data.notation.protobuf.confluent.ConfluentProtobufFileElementDescriptorMapper;

class ConfluentProtobufUnknownFieldsTest extends AbstractProtobufUnknownFieldsTest {
    @Override
    protected ProtobufFileElementDescriptorMapper descriptorMapper() {
        return new ConfluentProtobufFileElementDescriptorMapper();
    }

    @Override
    protected Descriptors.Descriptor writerDescriptor(String proto, String messageName) {
        return new io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema(proto).toDescriptor(messageName);
    }
}
