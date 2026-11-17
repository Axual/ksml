package io.axual.ksml.data.notation.apicurio;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library - Apicurio common
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

import io.apicurio.registry.client.common.RegistryClientOptions;
import io.apicurio.registry.resolver.client.RegistryClientFacadeFactory;
import io.apicurio.registry.resolver.config.SchemaResolverConfig;
import io.axual.ksml.data.exception.DataException;
import io.vertx.core.Vertx;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static io.axual.ksml.data.notation.apicurio.ApicurioRegistryClientFactory.TLS_KEYSTORE_CERTIFICATE;
import static io.axual.ksml.data.notation.apicurio.ApicurioRegistryClientFactory.TLS_KEYSTORE_KEY;
import static io.axual.ksml.data.notation.apicurio.ApicurioRegistryClientFactory.TLS_KEYSTORE_LOCATION;
import static io.axual.ksml.data.notation.apicurio.ApicurioRegistryClientFactory.TLS_KEYSTORE_PASSWORD;
import static io.axual.ksml.data.notation.apicurio.ApicurioRegistryClientFactory.TLS_KEYSTORE_TYPE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ApicurioRegistryClientFactoryTest {
    private static final String URL = "https://registry:8443/apis/registry/v3";

    private static Map<String, Object> configs(Object... keyValues) {
        final var result = new HashMap<String, Object>();
        result.put(SchemaResolverConfig.REGISTRY_URL, URL);
        for (int index = 0; index < keyValues.length; index += 2)
            result.put(keyValues[index].toString(), keyValues[index + 1]);
        return result;
    }

    private static RegistryClientOptions options(Map<String, Object> configs) {
        return ApicurioRegistryClientFactory.clientOptions(new SchemaResolverConfig(configs), configs);
    }

    @Test
    @DisplayName("Apicurio still has the private method that builds the client options")
    void apicurioStillHasBuildClientOptions() {
        // The factory calls it by reflection, so an Apicurio upgrade that removes it must fail here
        assertThatCode(() -> RegistryClientFacadeFactory.class.getDeclaredMethod(ApicurioRegistryClientFactory.BUILD_CLIENT_OPTIONS, SchemaResolverConfig.class, Vertx.class))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("No client is created without a registry URL")
    void returnsNullWithoutUrl() {
        assertThat(ApicurioRegistryClientFactory.create(null)).isNull();
        assertThat(ApicurioRegistryClientFactory.create(new HashMap<>())).isNull();
    }

    @Test
    @DisplayName("Without a key store, Apicurio creates the client")
    void createsClientWithoutKeystore() {
        assertThat(ApicurioRegistryClientFactory.create(configs())).isNotNull();
    }

    @Test
    @DisplayName("A JKS key store is the default type, and the other Apicurio settings are kept")
    void appliesJksKeystore() {
        final var options = options(configs(
                TLS_KEYSTORE_LOCATION, "/certs/client.jks",
                TLS_KEYSTORE_PASSWORD, "secret",
                SchemaResolverConfig.AUTH_USERNAME, "alice",
                SchemaResolverConfig.AUTH_PASSWORD, "wonderland",
                SchemaResolverConfig.TLS_TRUSTSTORE_LOCATION, "/certs/trust.jks",
                SchemaResolverConfig.TLS_TRUSTSTORE_PASSWORD, "trust"));

        assertThat(options.getKeyStoreType()).isEqualTo(RegistryClientOptions.KeyStoreType.JKS);
        assertThat(options.getKeyStorePath()).isEqualTo("/certs/client.jks");
        assertThat(options.getKeyStorePassword()).isEqualTo("secret");
        assertThat(options.getRegistryUrl()).isEqualTo(URL);
        assertThat(options.getUsername()).isEqualTo("alice");
        assertThat(options.getTrustStoreType()).isEqualTo(RegistryClientOptions.TrustStoreType.JKS);
        assertThat(options.getTrustStorePath()).isEqualTo("/certs/trust.jks");
    }

    @Test
    @DisplayName("A PKCS12 key store is applied")
    void appliesPkcs12Keystore() {
        final var options = options(configs(
                TLS_KEYSTORE_TYPE, "pkcs12",
                TLS_KEYSTORE_LOCATION, "/certs/client.p12",
                TLS_KEYSTORE_PASSWORD, "secret"));

        assertThat(options.getKeyStoreType()).isEqualTo(RegistryClientOptions.KeyStoreType.PKCS12);
        assertThat(options.getKeyStorePath()).isEqualTo("/certs/client.p12");
    }

    @Test
    @DisplayName("A PEM key store takes file paths or PEM content")
    void appliesPemKeystore() {
        final var fromFiles = options(configs(
                TLS_KEYSTORE_TYPE, "PEM",
                TLS_KEYSTORE_CERTIFICATE, "/certs/client.crt",
                TLS_KEYSTORE_KEY, "/certs/client.key"));
        assertThat(fromFiles.getPemClientCertPath()).isEqualTo("/certs/client.crt");
        assertThat(fromFiles.getPemClientKeyPath()).isEqualTo("/certs/client.key");

        final var certificate = "-----BEGIN CERTIFICATE-----\nMII...\n-----END CERTIFICATE-----";
        final var key = "-----BEGIN PRIVATE KEY-----\nMII...\n-----END PRIVATE KEY-----";
        final var fromContent = options(configs(
                TLS_KEYSTORE_TYPE, "PEM",
                TLS_KEYSTORE_CERTIFICATE, certificate,
                TLS_KEYSTORE_KEY, key));
        assertThat(fromContent.getPemClientCertContent()).isEqualTo(certificate);
        assertThat(fromContent.getPemClientKeyContent()).isEqualTo(key);
    }

    @Test
    @DisplayName("Incomplete or unknown key store settings are rejected")
    void rejectsInvalidKeystoreSettings() {
        assertThatThrownBy(() -> ApicurioRegistryClientFactory.create(configs(TLS_KEYSTORE_PASSWORD, "secret", TLS_KEYSTORE_KEY, "/certs/client.key")))
                .isInstanceOf(DataException.class)
                .hasMessageContaining(TLS_KEYSTORE_LOCATION);
        assertThatThrownBy(() -> ApicurioRegistryClientFactory.create(configs(TLS_KEYSTORE_TYPE, "PEM", TLS_KEYSTORE_CERTIFICATE, "/certs/client.crt")))
                .isInstanceOf(DataException.class)
                .hasMessageContaining(TLS_KEYSTORE_KEY);
        assertThatThrownBy(() -> ApicurioRegistryClientFactory.create(configs(TLS_KEYSTORE_TYPE, "PEM", TLS_KEYSTORE_CERTIFICATE, "/certs/client.crt", TLS_KEYSTORE_KEY, "-----BEGIN PRIVATE KEY-----")))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("both be file paths or both be PEM content");
        assertThatThrownBy(() -> ApicurioRegistryClientFactory.create(configs(TLS_KEYSTORE_TYPE, "BKS", TLS_KEYSTORE_LOCATION, "/certs/client.bks")))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("Unknown " + TLS_KEYSTORE_TYPE);
    }
}
