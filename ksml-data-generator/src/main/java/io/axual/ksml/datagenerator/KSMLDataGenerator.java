package io.axual.ksml.datagenerator;

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
import io.axual.ksml.data.mapper.DataObjectConverter;
import io.axual.ksml.datagenerator.config.DataGeneratorConfig;
import io.axual.ksml.datagenerator.execution.ExecutableProducer;
import io.axual.ksml.datagenerator.execution.IntervalSchedule;
import io.axual.ksml.datagenerator.factory.KafkaClientFactory;
import io.axual.ksml.datagenerator.parser.ProducerDefinitionFileParser;
import io.axual.ksml.python.PythonContext;
import io.axual.ksml.python.PythonFunction;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

@Slf4j
public class KSMLDataGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(KSMLDataGenerator.class);
    private static final String DEFAULT_CONFIG_FILE_SHORT = "ksml-data-generator.yaml";
    private static final IntervalSchedule<ExecutableProducer> schedule = new IntervalSchedule<>();

    private static Map<String, String> getGenericConfigs() {
        Map<String, String> configs = new HashMap<>();
        configs.put(ACKS_CONFIG, "1");
        configs.put(RETRIES_CONFIG, "0");
        configs.put(RETRY_BACKOFF_MS_CONFIG, "1000");
        configs.put(RECONNECT_BACKOFF_MAX_MS_CONFIG, "1000");
        configs.put(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "10");
        configs.put(KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
        configs.put(VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
        configs.put("specific.avro.reader", "true");
        return configs;
    }

    public static void main(String[] args) {
        var configFileName = DEFAULT_CONFIG_FILE_SHORT;

        if (args.length > 0) {
            configFileName = args[0];
        }

        final var configFile = new File(configFileName);
        if (!configFile.exists()) {
            log.error("Configuration file '{}' not found", configFile);
            System.exit(1);
        }

        final DataGeneratorConfig config;
        try {
            final var mapper = new ObjectMapper(new YAMLFactory());
            config = mapper.readValue(configFile, DataGeneratorConfig.class);
            config.validate();
        } catch (IOException e) {
            log.error("An exception occurred while reading the configuration", e);
            System.exit(2);
            return;
        }

        log.info("Initializing Kafka backend");
        final var configs = new HashMap<>(getGenericConfigs());
        configs.putAll(config.getKafka());
        final var factory = new KafkaClientFactory(configs);

        // Read all producer definitions from the configured YAML files
        var notationLibrary = factory.getNotationLibrary();
        var context = new PythonContext(new DataObjectConverter(notationLibrary));
        var producers = new ProducerDefinitionFileParser(config.getKsml()).create(notationLibrary, context);

        // Load all functions into the Python context

        // Schedule all defined producers
        for (var entry : producers.entrySet()) {
            var target = entry.getValue().target();
            var name = entry.getKey();
            var gen = entry.getValue().generator();
            final var generator = gen.name() != null
                    ? PythonFunction.fromNamed(context, gen.name(), gen.definition())
                    : PythonFunction.fromAnon(context, name, gen.definition(), "ksml.generator." + name);
            var cond = entry.getValue().condition();
            final var condition = (cond != null && cond.definition() != null)
                    ? cond.name() != null
                    ? PythonFunction.fromNamed(context, cond.name(), cond.definition())
                    : PythonFunction.fromAnon(context, name, cond.definition(), "ksml.condition." + name)
                    : null;
            var keySerde = notationLibrary.get(target.keyType.notation()).getSerde(target.keyType.dataType(), true);
            var keySerializer = factory.wrapSerializer(keySerde.serializer());
            var valueSerde = notationLibrary.get(target.valueType.notation()).getSerde(target.valueType.dataType(), false);
            var valueSerializer = factory.wrapSerializer(valueSerde.serializer());
            var ep = new ExecutableProducer(notationLibrary, generator, condition, target.topic, target.keyType, target.valueType, keySerializer, valueSerializer);
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
