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

import io.apicurio.rest.client.auth.Auth;
import io.apicurio.rest.client.auth.BasicAuth;
import io.axual.ksml.data.notation.avro.AvroNotation;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ApicurioAvroNotationProviderTest {

    @Test
    @DisplayName("Provider exposes notation/vendor names for Apicurio Avro")
    void providerMetadata_isCorrect() {
        assertThat(new ApicurioAvroNotationProvider())
                .returns(AvroNotation.NOTATION_NAME, ApicurioAvroNotationProvider::notationName)
                .returns("apicurio", ApicurioAvroNotationProvider::vendorName);
    }

    @Test
    @DisplayName("buildAuth returns HTTP Basic auth when a username and password are set")
    void buildAuth_withUsernameAndPassword_returnsBasicAuth() {
        final Map<String, Object> config = new HashMap<>();
        config.put("apicurio.registry.url", "http://registry:8081/apis/registry/v2");
        config.put("apicurio.auth.username", "alice");
        config.put("apicurio.auth.password", "secret");

        final Auth auth = new ApicurioAvroNotationProvider().buildAuth(config);

        assertThat(auth).isInstanceOf(BasicAuth.class);
        final var basicAuth = (BasicAuth) auth;
        assertThat(basicAuth.getUsername()).isEqualTo("alice");
        assertThat(basicAuth.getPassword()).isEqualTo("secret");
    }

    @Test
    @DisplayName("buildAuth returns null when no username is set (behavior unchanged)")
    void buildAuth_withoutUsername_returnsNull() {
        final Map<String, Object> config = new HashMap<>();
        config.put("apicurio.registry.url", "http://registry:8081/apis/registry/v2");

        assertThat(new ApicurioAvroNotationProvider().buildAuth(config)).isNull();
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
        config.put("apicurio.registry.url", "http://registry:8081/apis/registry/v2");

        assertThat(new ApicurioAvroNotationProvider().createSrClient(config)).isNotNull();
    }

    @Test
    @DisplayName("createSrClient builds a client when a URL and a login are set")
    void createSrClient_withUrlAndAuth_returnsClient() {
        final Map<String, Object> config = new HashMap<>();
        config.put("apicurio.registry.url", "http://registry:8081/apis/registry/v2");
        config.put("apicurio.auth.username", "alice");
        config.put("apicurio.auth.password", "secret");

        assertThat(new ApicurioAvroNotationProvider().createSrClient(config)).isNotNull();
    }
}
