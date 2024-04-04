package io.axual.ksml.runner.backend;

/*-
 * ========================LICENSE_START=================================
 * KSML Runner
 * %%
 * Copyright (C) 2021 - 2023 Axual B.V.
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

import io.axual.ksml.client.producer.ResolvingProducer;
import io.axual.ksml.client.serde.ResolvingSerializer;
import io.axual.ksml.data.notation.NotationLibrary;
import io.axual.ksml.generator.TopologyDefinition;
import io.axual.ksml.python.PythonContext;
import io.axual.ksml.python.PythonFunction;
import lombok.Builder;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class KafkaProducerRunner implements Runner {
    private static final Logger log = LoggerFactory.getLogger(KafkaProducerRunner.class);
    private static final IntervalSchedule<ExecutableProducer> schedule = new IntervalSchedule<>();
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final AtomicBoolean hasFailed = new AtomicBoolean(false);
    private final AtomicBoolean stopRunning = new AtomicBoolean(false);
    private final Config config;

    @Builder
    public record Config(Map<String, TopologyDefinition> definitions, Map<String, String> kafkaConfig) {
    }

    public KafkaProducerRunner(Config config) {
        this.config = config;
    }

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

    public void run() {
        log.info("Starting Kafka producer(s)");
        isRunning.set(true);
        try {
            config.definitions.forEach((defName, definition) -> {
                // Set up the Python context for this definition
                final var context = new PythonContext();
                // Pre-register all functions in the Python context
                definition.functions().forEach((name, function) -> PythonFunction.forFunction(context, definition.namespace(), name, function));
                // Schedule all defined producers
                definition.producers().forEach((name, producer) -> {
                    var target = producer.target();
                    var gen = producer.generator();
                    final var generator = gen.name() != null
                            ? PythonFunction.forGenerator(context, definition.namespace(), gen.name(), gen)
                            : PythonFunction.forGenerator(context, definition.namespace(), name, gen);
                    var cond = producer.condition();
                    final var condition = cond != null
                            ? cond.name() != null
                            ? PythonFunction.forPredicate(context, definition.namespace(), cond.name(), cond)
                            : PythonFunction.forPredicate(context, definition.namespace(), name, cond)
                            : null;
                    var keySerde = NotationLibrary.get(target.keyType().notation()).serde(target.keyType().dataType(), true);
                    var keySerializer = new ResolvingSerializer<>(keySerde.serializer(), config.kafkaConfig);
                    var valueSerde = NotationLibrary.get(target.valueType().notation()).serde(target.valueType().dataType(), false);
                    var valueSerializer = new ResolvingSerializer<>(valueSerde.serializer(), config.kafkaConfig);
                    var ep = new ExecutableProducer(generator, condition, target.topic(), target.keyType(), target.valueType(), keySerializer, valueSerializer);
                    if (producer.interval() == null) {
                        // no interval: schedule single shot produce
                        schedule.schedule(ep);
                    } else {
                        schedule.schedule(producer.interval().toMillis(), ep);
                    }
                    log.info("Scheduled producer: {}", name);
                });
            });

            try (final Producer<byte[], byte[]> producer = new ResolvingProducer<>(getProducerConfigs())) {
                while (!stopRunning.get()) {
                    try {
                        var generator = schedule.getScheduledItem();
                        while (generator != null) {
                            log.info("Calling " + generator.name());
                            generator.produceMessage(producer);
                            generator = schedule.getScheduledItem();
                        }
                        Utils.sleep(10);
                    } catch (Exception e) {
                        log.info("Produce exception.", e);
                        hasFailed.set(true);
                        break;
                    }
                }
            } catch (Throwable e) {
                hasFailed.set(true);
                log.error("Unhandled producer exception", e);
            }
        } catch (Throwable e) {
            hasFailed.set(true);
            log.error("Unhandled producer exception", e);
        }
        isRunning.set(false);
        log.info("Producer(s) stopped");
    }

    private Map<String, Object> getProducerConfigs() {
        final var result = new HashMap<String, Object>(config.kafkaConfig);
        result.put(KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        result.put(VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        return result;
    }

    @Override
    public State getState() {
        if (hasFailed.get()) return State.FAILED;
        if (isRunning.get()) return State.STARTED;
        return State.STOPPED;
    }

    @Override
    public void stop() {
        stopRunning.set(true);
    }
}
