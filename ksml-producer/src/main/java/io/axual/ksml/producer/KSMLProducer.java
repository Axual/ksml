package io.axual.ksml.producer;

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

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import io.axual.ksml.producer.config.KSMLProducerConfig;
import io.axual.ksml.producer.definition.ProducerDefinition;
import io.axual.ksml.producer.execution.ExecutableProducer;
import io.axual.ksml.producer.execution.IntervalSchedule;
import io.axual.ksml.producer.factory.AxualClientFactory;
import io.axual.ksml.producer.factory.ClientFactory;
import io.axual.ksml.producer.factory.KafkaClientFactory;
import io.axual.ksml.producer.parser.ProducerDefinitionFileParser;
import io.axual.ksml.python.PythonContext;
import io.axual.ksml.python.PythonFunction;
import lombok.extern.slf4j.Slf4j;

import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION;
import static org.apache.kafka.clients.producer.ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.RETRY_BACKOFF_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

@Slf4j
public class KSMLProducer {
    private static final Logger LOG = LoggerFactory.getLogger(KSMLProducer.class);
    private static final String DEFAULT_CONFIG_FILE_SHORT = "ksml-producer.yml";
    private static final PythonContext context = new PythonContext();
    private static final IntervalSchedule<ExecutableProducer> schedule = new IntervalSchedule<>();

    private static Map<String, Object> getGenericConfigs() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ACKS_CONFIG, "1");
        configs.put(RETRIES_CONFIG, "0");
        configs.put(RETRY_BACKOFF_MS_CONFIG, "1000");
        configs.put(RECONNECT_BACKOFF_MAX_MS_CONFIG, "1000");
        configs.put(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "10");
        configs.put(KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        configs.put(VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        configs.put("specific.avro.reader", true);
        return configs;
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

        final KSMLProducerConfig config;
        try {
            final var mapper = new ObjectMapper(new YAMLFactory());
            config = mapper.readValue(configFile, KSMLProducerConfig.class);
        } catch (IOException e) {
            log.error("An exception occurred while reading the configuration", e);
            System.exit(2);
            return;
        }

        final ClientFactory factory;
        if (useAxual) {
            log.info("Using Axual backend");
            factory = new AxualClientFactory(config.getAxual(), getGenericConfigs());
        } else {
            log.info("Using Kafka backend");
            factory = new KafkaClientFactory(config.getKafka(), getGenericConfigs());
        }

        // Read all producer definitions from the configured YAML files
        Map<String, ProducerDefinition> producers = new ProducerDefinitionFileParser(config.getProducer()).create(factory.getNotationLibrary());

        // Schedule all defined producers
        for (var entry : producers.entrySet()) {
            var target = entry.getValue().target();
            var generator = new PythonFunction(context, entry.getKey(), entry.getValue().generator());
            var keySerde = factory.getNotationLibrary().get(target.keyType.notation()).getSerde(target.keyType.dataType(), true);
            var valueSerde = factory.getNotationLibrary().get(target.valueType.notation()).getSerde(target.valueType.dataType(), false);
            var ep = new ExecutableProducer(generator, target.topic, target.keyType.dataType(), target.valueType.dataType(), keySerde.serializer(), valueSerde.serializer());
            schedule.schedule(entry.getValue().interval().toMillis(), ep);
            LOG.info("Scheduled producers: {}", entry.getKey());
        }

        try (final Producer<byte[], byte[]> producer = factory.getProducer()) {
            var interrupted = false;

            while (!interrupted) {
                try {
                    var generator = schedule.getScheduledItem();
                    while (generator != null) {
                        generator.produceMessage(producer);
                        generator = schedule.getScheduledItem();
                    }
                    Utils.sleep(10);
                } catch (Exception e) {
                    LOG.info("Interrupted: {}", e.getMessage());
                    e.printStackTrace();
                    interrupted = true;
                }
            }
        } finally {
            log.info("Done!");
        }
    }
}
