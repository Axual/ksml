package io.axual.ksml.example.producer.factory;

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

import io.axual.client.proxy.axual.admin.AxualAdminClient;
import io.axual.client.proxy.axual.admin.AxualAdminConfig;
import io.axual.client.proxy.axual.consumer.AxualConsumerConfig;
import io.axual.client.proxy.axual.producer.AxualProducer;
import io.axual.client.proxy.axual.producer.AxualProducerConfig;
import io.axual.client.proxy.generic.registry.ProxyChain;
import io.axual.common.config.CommonConfig;
import io.axual.ksml.example.producer.config.axual.AxualBackendConfig;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.config.SslConfigs;

import java.util.Map;

import static io.axual.client.proxy.generic.registry.ProxyTypeRegistry.*;
import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;

public class AxualClientFactory implements ClientFactory {
    private final AxualBackendConfig backendConfig;

    public AxualClientFactory(AxualBackendConfig config) {
        this.backendConfig = config;
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
    }

    @Override
    public <V> Producer<String, V> createProducer(Map<String, Object> configs) {
        addAxualClientProperties(configs);

        final String PREFIX = "schema.registry.";
        configs.put(PREFIX + SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, backendConfig.getSslConfig().getKeystoreLocation());
        configs.put(PREFIX + SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, backendConfig.getSslConfig().getKeystorePassword());
        configs.put(PREFIX + SslConfigs.SSL_KEY_PASSWORD_CONFIG, backendConfig.getSslConfig().getKeyPassword());
        configs.put(PREFIX + SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, backendConfig.getSslConfig().getTruststoreLocation());
        configs.put(PREFIX + SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, backendConfig.getSslConfig().getTruststorePassword());
        configs.put(PREFIX + SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
        configs.put(PREFIX + SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, "TLSv1.3,TLSv1.2,TLSv1.1,TLSv1");
        return new AxualProducer<>(configs);
    }

    @Override
    public Admin createAdmin(Map<String, Object> configs) {
        addAxualClientProperties(configs);
        return new AxualAdminClient(configs);
    }
}
