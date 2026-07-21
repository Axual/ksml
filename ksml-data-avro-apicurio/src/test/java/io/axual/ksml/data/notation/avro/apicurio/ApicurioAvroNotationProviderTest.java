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

import io.apicurio.registry.resolver.config.SchemaResolverConfig;
import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.notation.NotationContext;
import io.axual.ksml.data.notation.avro.AvroNotation;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ApicurioAvroNotationProviderTest {

    @Test
    @DisplayName("Provider exposes notation/vendor names for Apicurio Avro")
    void providerMetadata_isCorrect() {
        assertThat(new ApicurioAvroNotationProvider())
                .returns(AvroNotation.NOTATION_NAME, ApicurioAvroNotationProvider::notationName)
                .returns("apicurio", ApicurioAvroNotationProvider::vendorName);
    }

    @Test
    @DisplayName("createSrClient returns null when no registry URL is configured")
    void createSrClient_withoutRegistryUrl_returnsNull() {
        assertThat(new ApicurioAvroNotationProvider().createSrClient(new HashMap<>())).isNull();
    }

    @Test
    @DisplayName("createSrClient builds a client when a URL is set but no login")
    void createSrClient_withUrlNoAuth_returnsClient() {
        final Map<String, Object> config = new HashMap<>();
        config.put("apicurio.registry.url", "http://registry:8081/apis/registry/v3");

        assertThat(new ApicurioAvroNotationProvider().createSrClient(config)).isNotNull();
    }

    @Test
    @DisplayName("createSrClient builds a client when a URL and a login are set")
    void createSrClient_withUrlAndAuth_returnsClient() {
        final Map<String, Object> config = new HashMap<>();
        config.put("apicurio.registry.url", "http://registry:8081/apis/registry/v3");
        config.put("apicurio.registry.auth.username", "alice");
        config.put("apicurio.registry.auth.password", "secret");

        assertThat(new ApicurioAvroNotationProvider().createSrClient(config)).isNotNull();
    }

    @Test
    @DisplayName("Basic-auth credentials are read from the Apicurio v3 apicurio.registry.auth.* keys")
    void authCredentialsUseApicurioV3Keys() {
        // KSML no longer maps auth itself (main's buildAuth); it passes the config straight to Apicurio,
        // which reads the login from these keys. Apicurio v3 renamed them from apicurio.auth.* (v2) to
        // apicurio.registry.auth.*, so this pins the exact keys users must configure.
        final Map<String, Object> config = new HashMap<>();
        config.put("apicurio.registry.url", "http://registry:8081/apis/registry/v3");
        config.put("apicurio.registry.auth.username", "alice");
        config.put("apicurio.registry.auth.password", "secret");

        final var resolverConfig = new SchemaResolverConfig(config);

        assertThat(resolverConfig.getAuthUsername()).isEqualTo("alice");
        assertThat(resolverConfig.getAuthPassword()).isEqualTo("secret");
    }

    @Test
    @DisplayName("createNotation fails fast on the deprecated Apicurio v2 auth keys")
    void createNotation_withDeprecatedV2AuthKeys_throws() {
        final Map<String, String> config = new HashMap<>();
        config.put("apicurio.auth.username", "alice");
        final var prov = new ApicurioAvroNotationProvider();
        final var ctx = new NotationContext(config);
        assertThatThrownBy(() -> prov.createNotation(ctx))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("apicurio.auth.username")
                .hasMessageContaining(SchemaResolverConfig.AUTH_USERNAME);
    }
}
