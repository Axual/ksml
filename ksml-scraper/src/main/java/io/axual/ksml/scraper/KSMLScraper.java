package io.axual.ksml.scraper;

/*-
 * ========================LICENSE_START=================================
 * KSML Runner
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


import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static java.util.stream.Collectors.toSet;
import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.*;

@Slf4j
public class KSMLScraper {
    private static final long OFFSET_REQUEST_TIMEOUT_MS = 10_000;

    @AllArgsConstructor
    @Getter
    public static class KafkaConfiguration {
        private String securityProtocol;
        private String sslCertificate;
        private String sslKeyPassword;
        private String sslKeystoreLocation;
        private String sslKeystorePassword;
        private String sslTruststoreLocation;
        private String sslTruststorePassword;
        private String sslEndpointIdentificationAlgorithm;
        private int defaultReplicationFactor = 1;
        private long maxTopicViewRecords = 1000;

        public Properties asProperties() {
            final Properties props = new Properties();
            if (getSecurityProtocol() != null) {
                props.setProperty(SECURITY_PROTOCOL_CONFIG, getSecurityProtocol());
                props.setProperty(SSL_KEY_PASSWORD_CONFIG, getSslKeyPassword());
                props.setProperty(SSL_KEYSTORE_LOCATION_CONFIG, getSslKeystoreLocation());
                props.setProperty(SSL_KEYSTORE_PASSWORD_CONFIG, getSslKeystorePassword());
                props.setProperty(SSL_TRUSTSTORE_LOCATION_CONFIG, getSslTruststoreLocation());
                props.setProperty(SSL_TRUSTSTORE_PASSWORD_CONFIG, getSslTruststorePassword());
                props.setProperty(SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, getSslEndpointIdentificationAlgorithm());
            }
            return props;
        }
    }

    @AllArgsConstructor
    @Getter
    public static class InstanceInfo {
        private String instance;
        private String tenant;
    }

    @AllArgsConstructor
    @Getter
    public static class ClusterConnection {
        private String name;
        private Set<String> bootstrapServers;
        private List<InstanceInfo> instances;

        public String getBootstrapServersForKafka() {
            return String.join(",", bootstrapServers);
        }
    }

    private static final KafkaConfiguration kafkaConfiguration = new KafkaConfiguration(
            "SSL",
            "",
            "key_app-one",
            "/Users/dizzl/dev/axual/ksml/workspace/axualdemo-content/billing/ksml/app-one.client.keystore.jks",
            "keystore_app-one",
            "/Users/dizzl/dev/axual/ksml/workspace/axualdemo-content/billing/ksml/app-one.client.truststore.jks",
            "truststore_app-one",
            "",
            1,
            1000);

    public static KafkaConsumer<byte[], byte[]> getBinaryConsumer(String bootstrapServers) {
        Properties properties = new Properties();
        properties.putAll(kafkaConfiguration.asProperties());
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "billing-api");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return new KafkaConsumer<>(properties);
    }

    public static KafkaProducer<String, String> getStringProducer(String bootstrapServers) {
        Properties properties = new Properties();
        properties.putAll(kafkaConfiguration.asProperties());
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(properties);
    }

    private static AdminClient adminClient = null;

    public static Set<String> getAllTopics(String bootstrapServers) throws InterruptedException, ExecutionException {
        if (adminClient == null) {
            Properties props = kafkaConfiguration.asProperties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            log.debug("Setting up admin client with properties: {}", props);
            adminClient = AdminClient.create(props);
        }
        return adminClient.listTopics().names().get();
    }

    private static KafkaConsumer<byte[], byte[]> binaryConsmer = null;

    public static Map<String, Map<TopicPartition, Long>> getTopicEndOffsets(List<ClusterConnection> clusterConnectionsList) throws ExecutionException, InterruptedException {
        Map<String, Map<TopicPartition, Long>> result = new HashMap<>();
        log.debug("Cluster(s) size: {}", clusterConnectionsList.size());
        for (ClusterConnection clusterConnection : clusterConnectionsList) {
            try {
                final Map<TopicPartition, Long> endOffsets = result.computeIfAbsent(clusterConnection.getName(), name -> new HashMap<>());
                final Set<String> topics = getAllTopics(clusterConnection.getBootstrapServersForKafka());

                if (binaryConsmer == null) {
                    binaryConsmer = getBinaryConsumer(clusterConnection.getBootstrapServersForKafka());
                }

                log.debug("Topic(s) size: {}", topics.size());
                for (String topic : topics) {
                    Set<TopicPartition> partitions = binaryConsmer.partitionsFor(topic).stream().map(pi -> new TopicPartition(pi.topic(), pi.partition())).collect(toSet());
                    endOffsets.putAll(binaryConsmer.endOffsets(partitions, Duration.ofMillis(OFFSET_REQUEST_TIMEOUT_MS)));
                }
            } catch (Exception e) {
                log.error("Error in getting cluster instance message count for cluster {} with reason ", clusterConnection, e);
                throw e;
            }
        }
        log.debug("Returning the Map: {}", result);
        return result;
    }

    public static void main(String[] args) {
        var c = new ClusterConnection(
                "ROKIN",
                Collections.singleton("bootstrap-c1-ams01-azure.axual.cloud:9094"),
                Collections.singletonList(new InstanceInfo("dta", "axualdemo"))
        );
        var startTime = System.currentTimeMillis();
        while (true) {
            try {
                var offsets = getTopicEndOffsets(Collections.singletonList(c));
                var producer = getStringProducer(c.getBootstrapServersForKafka());
                for (var entry : offsets.entrySet()) {
                    for (var partition : entry.getValue().entrySet()) {
                        var key = "{\"timestamp\": %d, \"cluster\": \"%s\", \"topic\": \"%s\", \"partition\": %d}";
                        var value = "{\"value\": %d}";
                        var record = new ProducerRecord<String, String>(
                                "axualdemo-dta-billingdemo-offsets",
                                String.format(key, System.currentTimeMillis(), c.name, partition.getKey().topic(), partition.getKey().partition()),
                                String.format(value, partition.getValue())
                        );
                        producer.send(record);
                    }
                    var now = System.currentTimeMillis();
                    System.out.println("Produced offsets for " + entry.getValue().entrySet().size() + " topics on " + entry.getKey() + ", started at " + startTime + " and done in " + (now - startTime) + " ms");
                }
                var timeTaken = System.currentTimeMillis() - startTime;
                if (timeTaken < 5000) {
                    Thread.sleep(5000 - timeTaken);
                } else {
                    Thread.sleep(3000);
                }
                startTime = System.currentTimeMillis();
            } catch (Exception e) {
                if (adminClient != null) {
                    adminClient.close();
                    adminClient = null;
                }
                if (binaryConsmer != null) {
                    binaryConsmer.close();
                    binaryConsmer = null;
                }
                System.out.println("Exception: " + e.getMessage());
            }
        }
    }
}
