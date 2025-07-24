package io.axual.ksml.data.notation.avro.confluent;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library - AVRO Confluent
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
import io.axual.ksml.data.notation.avro.AvroNotation;
import io.axual.ksml.data.notation.base.VendorNotationProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

public class ConfluentAvroNotationProvider extends VendorNotationProvider {
    private final SchemaRegistryClient registryClient;

    public ConfluentAvroNotationProvider() {
        this(null);
    }

    public ConfluentAvroNotationProvider(SchemaRegistryClient registryClient) {
        super(AvroNotation.NOTATION_NAME, "confluent");
        this.registryClient = registryClient;
    }

    public Notation createNotation() {
        return createNotation(new NotationContext());
    }

    @Override
    public Notation createNotation(NotationContext notationContext) {
        return new AvroNotation(new ConfluentAvroSerdeSupplier(notationContext, registryClient));
    }
}
