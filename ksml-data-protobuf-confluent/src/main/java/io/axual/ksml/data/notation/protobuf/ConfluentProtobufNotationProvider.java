package io.axual.ksml.data.notation.protobuf;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library - Protobuf Confluent
 * %%
 * Copyright (C) 2021 - 2025 Axual B.V.
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

import io.axual.ksml.data.notation.Notation;
import io.axual.ksml.data.notation.NotationContext;
import io.axual.ksml.data.notation.vendor.VendorNotationContext;
import io.axual.ksml.data.notation.vendor.VendorNotationProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

public class ConfluentProtobufNotationProvider extends VendorNotationProvider {
    private final SchemaRegistryClient registryClient;

    public ConfluentProtobufNotationProvider() {
        this(null);
    }

    public ConfluentProtobufNotationProvider(SchemaRegistryClient registryClient) {
        super(ProtobufNotation.NOTATION_NAME, "confluent");
        this.registryClient = registryClient;
    }

    @Override
    public Notation createNotation(NotationContext context) {
        return new ProtobufNotation(
                new VendorNotationContext(
                        context,
                        new ConfluentProtobufSerdeSupplier(registryClient),
                        new ProtobufDataObjectMapper(new ConfluentProtobufDescriptorFileElementMapper())),
                new ConfluentProtobufSchemaParser());
    }
}
