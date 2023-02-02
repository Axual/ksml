package io.axual.ksml.datagenerator.factory;

/*-
 * ========================LICENSE_START=================================
 * KSML Example Producer
 * %%
 * Copyright (C) 2021 Axual B.V.
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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.types.Password;

import java.util.HashMap;
import java.util.Map;

import io.axual.client.proxy.axual.admin.AxualAdminClient;
import io.axual.client.proxy.axual.admin.AxualAdminConfig;
import io.axual.client.proxy.axual.consumer.AxualConsumerConfig;
import io.axual.client.proxy.axual.producer.AxualProducer;
import io.axual.client.proxy.axual.producer.AxualProducerConfig;
import io.axual.client.proxy.generic.registry.ProxyChain;
import io.axual.common.config.ClientConfig;
import io.axual.common.config.CommonConfig;
import io.axual.common.config.SslConfig;
import io.axual.common.tools.KafkaUtil;
import io.axual.discovery.client.DiscoveryClientRegistry;
import io.axual.discovery.client.DiscoveryResult;
import io.axual.discovery.client.exception.DiscoveryClientRegistrationException;
import io.axual.discovery.client.tools.DiscoveryConfigParserV2;
import io.axual.ksml.AxualNotationLibrary;
import io.axual.ksml.exception.KSMLTopologyException;
import io.axual.ksml.notation.NotationLibrary;
import io.axual.ksml.datagenerator.config.axual.AxualBackendConfig;

import static io.axual.client.proxy.generic.registry.ProxyTypeRegistry.HEADER_PROXY_ID;
import static io.axual.client.proxy.generic.registry.ProxyTypeRegistry.LINEAGE_PROXY_ID;
import static io.axual.client.proxy.generic.registry.ProxyTypeRegistry.RESOLVING_PROXY_ID;
import static io.axual.client.proxy.generic.registry.ProxyTypeRegistry.SWITCHING_PROXY_ID;
import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;

public class AxualClientFactory implements ClientFactory {
    private final AxualBackendConfig backendConfig;
    private final Map<String, Object> clientConfigs;
    private DiscoveryResult discoveryResult;
    private final NotationLibrary notationLibrary;

    public AxualClientFactory(AxualBackendConfig backendConfig, Map<String, Object> clientConfigs) {
        this.backendConfig = backendConfig;
        this.clientConfigs = new HashMap<>(clientConfigs);
        addAxualClientProperties(this.clientConfigs);

        var clientConfig = ClientConfig.newBuilder()
                .setTenant(backendConfig.getTenant())
                .setEnvironment(backendConfig.getEnvironment())
                .setEndpoint(backendConfig.getEndpoint())
                .setApplicationId(backendConfig.getApplicationId())
                .setApplicationVersion(backendConfig.getApplicationVersion())
                .setSslConfig(SslConfig.newBuilder()
                        .setEnableHostnameVerification(backendConfig.getSslConfig().isEnableHostnameVerification())
                        .setKeystoreLocation(backendConfig.getSslConfig().getKeystoreLocation())
                        .setKeystorePassword(backendConfig.getSslConfig().getKeystorePassword())
                        .setKeyPassword(backendConfig.getSslConfig().getKeyPassword())
                        .setTruststoreLocation(backendConfig.getSslConfig().getTruststoreLocation())
                        .setTruststorePassword(backendConfig.getSslConfig().getTruststorePassword())
                        .build())
                .build();

        var discoveryConfig = DiscoveryConfigParserV2.getDiscoveryConfig(clientConfig);
        try {
            DiscoveryClientRegistry.register(discoveryConfig, this::discoveryPropertiesChanged);
            DiscoveryClientRegistry.checkProperties(discoveryConfig);
        } catch (DiscoveryClientRegistrationException e) {
            throw new KSMLTopologyException("Axual discovery service registration failed", e);
        }

        Map<String, Object> configs = new HashMap<>(clientConfigs);
        configs.putAll(KafkaUtil.getKafkaConfigs(clientConfig));
        configs.putAll(discoveryResult.getConfigs());
        // Copy the SSL configuration so the schema registry also gets it as its config
        Map<String, Object> copy = new HashMap<>(configs);
        copy.keySet().stream().filter(k -> k.startsWith("ssl.")).forEach(k -> {
            final Object value = copy.get(k);
            // Explode passwords into their string literals
            if (value instanceof Password password) {
                configs.put("schema.registry." + k, password.value());
            } else {
                configs.put("schema.registry." + k, value);
            }
        });
        notationLibrary = new AxualNotationLibrary(configs);
    }

    public void discoveryPropertiesChanged(DiscoveryResult newDiscoveryResult) {
        discoveryResult = newDiscoveryResult;
    }

    void addAxualClientProperties(Map<String, Object> configs) {
        ProxyChain chain = ProxyChain.newBuilder()
                .append(SWITCHING_PROXY_ID)
                .append(RESOLVING_PROXY_ID)
                .append(LINEAGE_PROXY_ID)
                .append(HEADER_PROXY_ID)
                .build();
        configs.put(AxualProducerConfig.CHAIN_CONFIG, chain);
        configs.put(AxualConsumerConfig.CHAIN_CONFIG, chain);
        configs.put(AxualAdminConfig.CHAIN_CONFIG, chain);
        configs.put(CommonConfig.APPLICATION_ID, backendConfig.getApplicationId());
        configs.put(CommonConfig.APPLICATION_VERSION, backendConfig.getApplicationVersion());
        configs.put(CommonConfig.TENANT, backendConfig.getTenant());
        configs.put(CommonConfig.ENVIRONMENT, backendConfig.getEnvironment());

        configs.put(BOOTSTRAP_SERVERS_CONFIG, backendConfig.getEndpoint());
        configs.put(SECURITY_PROTOCOL_CONFIG, "SSL");
        configs.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, backendConfig.getSslConfig().getKeystoreLocation());
        configs.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, backendConfig.getSslConfig().getKeystorePassword());
        configs.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, backendConfig.getSslConfig().getKeyPassword());
        configs.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, backendConfig.getSslConfig().getTruststoreLocation());
        configs.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, backendConfig.getSslConfig().getTruststorePassword());
        configs.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
        configs.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, "TLSv1.3,TLSv1.2,TLSv1.1,TLSv1");

        final String PREFIX = "schema.registry.";
        configs.put(PREFIX + SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, backendConfig.getSslConfig().getKeystoreLocation());
        configs.put(PREFIX + SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, backendConfig.getSslConfig().getKeystorePassword());
        configs.put(PREFIX + SslConfigs.SSL_KEY_PASSWORD_CONFIG, backendConfig.getSslConfig().getKeyPassword());
        configs.put(PREFIX + SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, backendConfig.getSslConfig().getTruststoreLocation());
        configs.put(PREFIX + SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, backendConfig.getSslConfig().getTruststorePassword());
        configs.put(PREFIX + SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
        configs.put(PREFIX + SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, "TLSv1.3,TLSv1.2,TLSv1.1,TLSv1");
    }

    @Override
    public Producer<byte[], byte[]> getProducer() {
        return new AxualProducer<>(clientConfigs);
    }

    @Override
    public Admin getAdmin() {
        return new AxualAdminClient(clientConfigs);
    }

    @Override
    public NotationLibrary getNotationLibrary() {
        return notationLibrary;
    }
}
