package io.axual.ksml.example.producer;

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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.axual.ksml.example.SensorAlertSettings;
import io.axual.ksml.example.SensorData;
import io.axual.ksml.example.producer.config.ExampleProducerConfig;
import io.axual.ksml.example.producer.factory.AxualClientFactory;
import io.axual.ksml.example.producer.factory.ClientFactory;
import io.axual.ksml.example.producer.factory.KafkaClientFactory;
import io.axual.ksml.example.producer.generator.SensorAlertSettingGenerator;
import io.axual.ksml.example.producer.generator.SensorDataGenerator;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

@Slf4j
public class KSMLExampleProducer {
    private static final String DEFAULT_CONFIG_FILE_SHORT = "ksml-example-producer.yml";
    public static final String TOPIC_SENSORDATA = "ksml_sensordata_avro";
    public static final String TOPIC_SENSORALERTSETTINGS = "ksml_sensoralert_settings";

    private static Map<String, Object> getGenericConfigs() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ACKS_CONFIG, "1");
        configs.put(RETRIES_CONFIG, "0");
        configs.put(RETRY_BACKOFF_MS_CONFIG, "1000");
        configs.put(RECONNECT_BACKOFF_MAX_MS_CONFIG, "1000");
        configs.put(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "10");
        configs.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        configs.put("specific.avro.reader", true);
        return configs;
    }

    private static boolean produceMessage(Producer<String, SpecificRecord> producer, long iteration) {
        log.info("Producing: {}", iteration);
        String sensorName = "sensor" + (iteration % 10);
        String key = (iteration % 2) == 0 ? sensorName : null;
        Future<RecordMetadata> future = producer.send(
                new ProducerRecord<>(
                        "ksml_sensordata_avro",
                        key,
                        (iteration % 3) != 0 ? SensorDataGenerator.generateValue(sensorName) : null
                ));
        try {
            RecordMetadata message = future.get();
            log.info("Produced message to topic {} partition {} offset {}", message.topic(), message.partition(), message.offset());
            return true;
        } catch (InterruptedException | ExecutionException e) {
            log.error("Interrupted!", e);
            Thread.currentThread().interrupt();
            return false;
        }
    }

    public static void main(String[] args) {
        var useAxual = false;
        String configFileName = DEFAULT_CONFIG_FILE_SHORT;

        if (args.length > 0) {
            var configFileArg = 0;
            if ("-axual".equals(args[0])) {
                useAxual = true;
                configFileArg = 1;
            }

            if (args.length > configFileArg) {
                configFileName = args[configFileArg];
            }
        }

        final var configFile = new File(configFileName);
        if (!configFile.exists()) {
            log.error("Configuration file '{}' not found", configFile);
            System.exit(1);
        }

        final ExampleProducerConfig config;
        try {
            final var mapper = new ObjectMapper(new YAMLFactory());
            config = mapper.readValue(configFile, ExampleProducerConfig.class);
        } catch (IOException e) {
            log.error("An exception occurred while reading the configuration", e);
            System.exit(2);
            return;
        }

        log.info("Start producing messages");
        final ClientFactory factory;
        if (useAxual) {
            log.info("Using Axual backend");
            factory = new AxualClientFactory(config.getAxual());
        } else {
            log.info("Using Kafka backend");
            factory = new KafkaClientFactory(config.getKafka());
        }

        try (
                final Producer<String, SpecificRecord> producer = factory.createProducer(getGenericConfigs())
        ) {
            boolean interrupted = false;

            // Create Alert Settings
            for (String city : SensorDataGenerator.CITIES) {
                Future<RecordMetadata> future = producer.send(
                        new ProducerRecord<>(
                                TOPIC_SENSORALERTSETTINGS,
                                city,
                                SensorAlertSettingGenerator.generateAlertSettings(city)
                        )
                );
                try {
                    RecordMetadata message = future.get();
                    log.info("Produced alert setting to topic {} partition {} offset {}", message.topic(), message.partition(), message.offset());
                    Utils.sleep(500);
                } catch (InterruptedException | ExecutionException e) {
                    log.error("Interrupted!", e);
                    interrupted = true;
                    Thread.currentThread().interrupt();
                }

                long index = 0;
                while (produceMessage(producer, index)) {
                    Utils.sleep(500);
                    index++;
                }
            }

        } finally {
            log.info("Done!");
        }
    }
}
