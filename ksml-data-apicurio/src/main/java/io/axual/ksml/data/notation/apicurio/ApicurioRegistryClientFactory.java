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

import io.apicurio.registry.client.RegistryClientFactory;
import io.apicurio.registry.client.common.RegistryClientOptions;
import io.apicurio.registry.resolver.client.RegistryClientFacade;
import io.apicurio.registry.resolver.client.RegistryClientFacadeFactory;
import io.apicurio.registry.resolver.client.RegistryClientFacadeImpl;
import io.apicurio.registry.resolver.config.SchemaResolverConfig;
import io.axual.ksml.data.exception.DataException;
import io.vertx.core.Vertx;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

/**
 * Creates the Apicurio registry client for the Apicurio-backed notations.
 *
 * <p>Apicurio v3's client supports mutual TLS, but its serdes offer no setting for the client certificate. This
 * factory adds the settings below. Without them, it leaves the client to Apicurio.</p>
 * <ul>
 *     <li>{@value #TLS_KEYSTORE_TYPE}: JKS (default), PKCS12 or PEM</li>
 *     <li>{@value #TLS_KEYSTORE_LOCATION} and {@value #TLS_KEYSTORE_PASSWORD}: the JKS or PKCS12 key store. The
 *     password also unlocks the private key.</li>
 *     <li>{@value #TLS_KEYSTORE_CERTIFICATE} and {@value #TLS_KEYSTORE_KEY}: the PEM client certificate and private
 *     key, each as a file path or as PEM content</li>
 * </ul>
 */
public final class ApicurioRegistryClientFactory {
    public static final String TLS_KEYSTORE_TYPE = "apicurio.registry.tls.keystore.type";
    public static final String TLS_KEYSTORE_LOCATION = "apicurio.registry.tls.keystore.location";
    public static final String TLS_KEYSTORE_PASSWORD = "apicurio.registry.tls.keystore.password";
    public static final String TLS_KEYSTORE_CERTIFICATE = "apicurio.registry.tls.keystore.certificate";
    public static final String TLS_KEYSTORE_KEY = "apicurio.registry.tls.keystore.key";
    private static final String TLS_KEYSTORE_TYPE_DEFAULT = "JKS";
    private static final String PEM_MARKER = "-----BEGIN";
    static final String BUILD_CLIENT_OPTIONS = "buildClientOptions";

    private ApicurioRegistryClientFactory() {
    }

    /**
     * Create the registry client for these serde configs.
     *
     * @param configs the serde configuration (nullable)
     * @return the client, or null when no registry URL is configured
     * @throws DataException if the key store settings are incomplete or unknown
     */
    public static RegistryClientFacade create(Map<String, Object> configs) {
        if (configs == null || !configs.containsKey(SchemaResolverConfig.REGISTRY_URL))
            return null;
        final var config = new SchemaResolverConfig(configs);
        if (!hasKeystore(configs))
            return RegistryClientFacadeFactory.create(config);

        // KSML 2.0 only supports the v3 registry API
        final var options = clientOptions(config, configs);
        return new RegistryClientFacadeImpl(RegistryClientFactory.create(options), config.getRegistryUrl(), config.getUsageTelemetryClientId());
    }

    // Apicurio's client options with the key store added. Package-private so tests can check the options without
    // a key store file: the client loads the file when it is created.
    static RegistryClientOptions clientOptions(SchemaResolverConfig config, Map<String, Object> configs) {
        final var options = buildClientOptions(config);
        applyKeystore(options, configs);
        return options;
    }

    private static boolean hasKeystore(Map<String, Object> configs) {
        return configs.containsKey(TLS_KEYSTORE_LOCATION) || configs.containsKey(TLS_KEYSTORE_CERTIFICATE) || configs.containsKey(TLS_KEYSTORE_KEY);
    }

    // Apicurio keeps the options for the URL, authentication, trust store, proxy and retries private. Calling it
    // keeps those settings exactly as Apicurio applies them, so only the key store is added here.
    // ApicurioRegistryClientFactoryTest checks that the method still exists.
    private static RegistryClientOptions buildClientOptions(SchemaResolverConfig config) {
        try {
            final var method = RegistryClientFacadeFactory.class.getDeclaredMethod(BUILD_CLIENT_OPTIONS, SchemaResolverConfig.class, Vertx.class);
            method.setAccessible(true);
            return (RegistryClientOptions) method.invoke(null, config, config.getVertx());
        } catch (InvocationTargetException e) {
            throw new DataException("Could not configure the Apicurio registry client: " + e.getCause().getMessage(), e.getCause());
        } catch (ReflectiveOperationException e) {
            throw new DataException("This Apicurio version does not support the " + TLS_KEYSTORE_LOCATION + " settings", e);
        }
    }

    private static void applyKeystore(RegistryClientOptions options, Map<String, Object> configs) {
        final var type = getString(configs, TLS_KEYSTORE_TYPE, TLS_KEYSTORE_TYPE_DEFAULT);
        final var password = getString(configs, TLS_KEYSTORE_PASSWORD, null);
        switch (type.toUpperCase()) {
            case "JKS" -> options.keystoreJks(require(configs, TLS_KEYSTORE_LOCATION, type), password);
            case "PKCS12", "P12" -> options.keystorePkcs12(require(configs, TLS_KEYSTORE_LOCATION, type), password);
            case "PEM" -> {
                final var certificate = require(configs, TLS_KEYSTORE_CERTIFICATE, type);
                final var key = require(configs, TLS_KEYSTORE_KEY, type);
                if (certificate.contains(PEM_MARKER) != key.contains(PEM_MARKER))
                    throw new DataException(TLS_KEYSTORE_CERTIFICATE + " and " + TLS_KEYSTORE_KEY + " must both be file paths or both be PEM content");
                if (certificate.contains(PEM_MARKER))
                    options.keystorePemContent(certificate, key);
                else
                    options.keystorePem(certificate, key);
            }
            default -> throw new DataException("Unknown " + TLS_KEYSTORE_TYPE + " '" + type + "', use JKS, PKCS12 or PEM");
        }
    }

    private static String require(Map<String, Object> configs, String key, String type) {
        final var value = getString(configs, key, null);
        if (value == null || value.isBlank())
            throw new DataException("Configuration key '" + key + "' is required for a " + type + " key store");
        return value;
    }

    private static String getString(Map<String, Object> configs, String key, String defaultValue) {
        final var value = configs.get(key);
        return value != null ? value.toString() : defaultValue;
    }
}
