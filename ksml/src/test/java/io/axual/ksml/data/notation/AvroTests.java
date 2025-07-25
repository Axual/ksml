package io.axual.ksml.data.notation;

/*-
 * ========================LICENSE_START=================================
 * KSML
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

import io.axual.ksml.data.notation.apicurio.MockApicurioSchemaRegistryClient;
import io.axual.ksml.data.notation.avro.AvroDataObjectMapper;
import io.axual.ksml.data.notation.avro.AvroNotation;
import io.axual.ksml.data.notation.avro.AvroSchemaMapper;
import io.axual.ksml.data.notation.avro.apicurio.ApicurioAvroNotationProvider;
import io.axual.ksml.data.notation.avro.confluent.ConfluentAvroNotationProvider;
import io.axual.ksml.data.notation.confluent.MockConfluentSchemaRegistryClient;
import org.junit.jupiter.api.Test;

class AvroTests {
    @Test
    void schemaTest() {
        NotationTestRunner.schemaTest(AvroNotation.NOTATION_NAME, new AvroSchemaMapper());
    }

    @Test
    void dataTest() {
        NotationTestRunner.dataTest(AvroNotation.NOTATION_NAME, new AvroDataObjectMapper());
    }

    @Test
    void apicurioSerdeTest() {
//        final var registryClient = new MockApicurioSchemaRegistryClient();
//        final var provider = new ApicurioAvroNotationProvider(registryClient);
//        final var context = new NotationContext(provider.notationName(), provider.vendorName(), registryClient.configs());
//        final var notation = provider.createNotation(context);
//        NotationTestRunner.serdeTest(notation, true);
    }

    @Test
    void confluentSerdeTest() {
        final var registryClient = new MockConfluentSchemaRegistryClient();
        final var provider = new ConfluentAvroNotationProvider(registryClient);
        final var context = new NotationContext(provider.notationName(), provider.vendorName(), registryClient.configs());
        final var notation = provider.createNotation(context);
        NotationTestRunner.serdeTest(notation, true);
    }
}
