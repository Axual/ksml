package io.axual.ksml.data.notation.avro.apicurio;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library - AVRO Apicurio
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

import io.apicurio.registry.resolver.client.RegistryClientFacade;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class ApicurioAvroSerdeSupplierTest {

    @Test
    @DisplayName("get() builds a Serde using the default Apicurio serializers when no client is set")
    void getWithoutRegistryClient() {
        assertThat(new ApicurioAvroSerdeSupplier(null).get(null, false)).isNotNull();
    }

    @Test
    @DisplayName("get() builds a Serde around the supplied registry client")
    void getWithRegistryClient() {
        final var client = mock(RegistryClientFacade.class);
        assertThat(new ApicurioAvroSerdeSupplier(client).get(null, true)).isNotNull();
    }
}
