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
import io.axual.ksml.data.notation.avro.AvroSerdeProvider;
import io.axual.ksml.data.notation.avro.apicurio.ApicurioAvroSerdeProvider;
import io.axual.ksml.data.notation.avro.confluent.ConfluentAvroSerdeProvider;
import io.axual.ksml.data.notation.confluent.MockConfluentSchemaRegistryClient;
import org.junit.jupiter.api.Test;

import java.util.Map;

class AvroTests {
    @Test
    void schemaTest() {
        NotationTestRunner.schemaTest(AvroNotation.NOTATION_NAME, new AvroSchemaMapper());
    }

    @Test
    void dataTest() {
        NotationTestRunner.dataTest(AvroNotation.NOTATION_NAME, new AvroDataObjectMapper());
    }

    void serdeTest(AvroSerdeProvider serdeProvider, Map<String, String> srConfigs) {
        final var notation = new AvroNotation(serdeProvider, new AvroDataObjectMapper(), srConfigs);
        NotationTestRunner.serdeTest(AvroNotation.NOTATION_NAME, notation, true);
    }

    @Test
    void apicurioSerdeTest() {
        final var registryClient = new MockApicurioSchemaRegistryClient();
        final var serdeProvider = new ApicurioAvroSerdeProvider(registryClient);
        serdeTest(serdeProvider, registryClient.configs());
    }

    @Test
    void confluentSerdeTest() {
        final var registryClient = new MockConfluentSchemaRegistryClient();
        final var serdeProvider = new ConfluentAvroSerdeProvider(registryClient);
        serdeTest(serdeProvider, registryClient.configs());
    }
}
