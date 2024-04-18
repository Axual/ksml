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

    public void run() {
        log.info("Registering Kafka producer(s)");
        isRunning.set(true);
        try {
            config.definitions.forEach((defName, definition) -> {
                // Set up the Python context for this definition
                final var context = new PythonContext();
                // Pre-register all functions in the Python context
                definition.functions().forEach((name, function) -> PythonFunction.forFunction(context, definition.namespace(), name, function));
                // Schedule all defined producers
                definition.producers().forEach((name, producer) -> {
                    var ep = ExecutableProducer.forProducer(context, definition.namespace(), name, producer, config.kafkaConfig);
                    schedule.schedule(ep);
                    log.info("Scheduled producer: {} {}", name, producer.interval() == null ? "once" : producer.interval().toMillis() + "ms");
                });
            });

            try (final Producer<byte[], byte[]> producer = createProducer(getProducerConfigs())) {
                log.info("starting Kafka producer(s)");
                while (!stopRunning.get()) {
                    try {
                        var generator = schedule.getScheduledItem();
                        while (generator != null) {
                            log.info("Calling {}", generator.name());
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


    /**
     * Creates a Kafka producer based on the provided config.
     * This method is package protected so we can override it for testing
     * @param producerConfig the producer configs.
     * @return a Kafka producer.
     */
    protected Producer<byte[], byte[]> createProducer(Map<String, Object> producerConfig) {
        return new ResolvingProducer<>(producerConfig);
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
