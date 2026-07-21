package io.axual.ksml.data.notation.protobuf.apicurio;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library - Protobuf Apicurio
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

import io.apicurio.registry.resolver.config.SchemaResolverConfig;
import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.notation.NotationContext;
import io.axual.ksml.data.notation.protobuf.ProtobufNotation;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ApicurioProtobufNotationProviderTest {

    @Test
    @DisplayName("Provider exposes notation/vendor names for Apicurio Protobuf")
    void providerMetadata_isCorrect() {
        assertThat(new ApicurioProtobufNotationProvider())
                .returns(ProtobufNotation.NOTATION_NAME, ApicurioProtobufNotationProvider::notationName)
                .returns("apicurio", ApicurioProtobufNotationProvider::vendorName);
    }

    @Test
    @DisplayName("createNotation fails fast on the deprecated Apicurio v2 auth keys")
    void createNotation_withDeprecatedV2AuthKeys_throws() {
        final Map<String, String> config = new HashMap<>();
        config.put("apicurio.auth.username", "alice");
        final var prov = new ApicurioProtobufNotationProvider();
        final var ctx = new NotationContext(config);
        assertThatThrownBy(() -> prov.createNotation(ctx))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("apicurio.auth.username")
                .hasMessageContaining(SchemaResolverConfig.AUTH_USERNAME);
    }
}
