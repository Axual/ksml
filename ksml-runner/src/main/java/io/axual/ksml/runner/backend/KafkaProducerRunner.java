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

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import io.axual.ksml.client.producer.ResolvingProducer;
import io.axual.ksml.client.resolving.ResolvingClientConfig;
import io.axual.ksml.generator.TopologyDefinition;
import io.axual.ksml.python.PythonContext;
import io.axual.ksml.python.PythonFunction;
import io.axual.ksml.runner.exception.RunnerException;
import io.axual.ksml.runner.producer.ExecutableProducer;
import io.axual.ksml.runner.producer.IntervalSchedule;
import lombok.Builder;

import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public class KafkaProducerRunner implements Runner {
    private static final Logger log = LoggerFactory.getLogger(KafkaProducerRunner.class);
    private final IntervalSchedule scheduler = new IntervalSchedule();
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final AtomicBoolean hasFailed = new AtomicBoolean(false);
    private final AtomicBoolean stopRunning = new AtomicBoolean(false);
    private final Config config;
    private State currentState;

    @Builder
    public record Config(Map<String, TopologyDefinition> definitions, Map<String, String> kafkaConfig) {
        public Config(final Map<String, TopologyDefinition> definitions, final Map<String, String> kafkaConfig) {
            var processedKafkaConfig = new HashMap<>(kafkaConfig);
            this.definitions = definitions;
            // Check if a resolving client is required
            if (ResolvingClientConfig.configRequiresResolving(processedKafkaConfig)) {
                log.info("Using resolving clients for producer processing");
                // Replace the deprecated configuration keys with the current ones
                ResolvingClientConfig.replaceDeprecatedConfigKeys(processedKafkaConfig);
            }
            this.kafkaConfig = processedKafkaConfig;
        }
    }

    public KafkaProducerRunner(Config config) {
        this.config = config;
        currentState = State.CREATED;
    }


    public synchronized void setState(State newState) {
        if (currentState.isValidNextState(newState)) {
            currentState = newState;
        } else {
            log.warn("Illegal Producer State transition. Current state is {}. Invalid next state {}", currentState, newState);
        }
    }

    public void run() {
        log.info("Registering Kafka producer(s)");
        isRunning.set(true);
        setState(State.STARTING);

        try {
            config.definitions.forEach((defName, definition) -> {
                // Set up the Python context for this definition
                final var context = new PythonContext();
                // Pre-register all functions in the Python context
                definition.functions().forEach((name, function) -> PythonFunction.forFunction(context, definition.namespace(), name, function));
                // Schedule all defined producers
                definition.producers().forEach((name, producer) -> {
                    var ep = ExecutableProducer.forProducer(context, definition.namespace(), name, producer, config.kafkaConfig);
                    scheduler.schedule(ep);
                    log.info("Scheduled producer: {} {}", name, producer.interval() == null ? "once" : producer.interval().toMillis() + "ms");
                });
            });
        } catch (Exception e) {
            setState(State.FAILED);
            throw new RunnerException("Error while registering functions and producers", e);
        }

        try (final Producer<byte[], byte[]> producer = createProducer(getProducerConfigs())) {
            setState(State.STARTED);
            log.info("Starting Kafka producer(s)");
            while (!stopRunning.get() && !hasFailed.get() && scheduler.hasScheduledItems()) {
                var scheduledGenerator = scheduler.getScheduledItem();
                if (scheduledGenerator != null) {
                    scheduledGenerator.producer().produceMessages(producer);
                    if (scheduledGenerator.producer().shouldReschedule()) {
                        final var interval = scheduledGenerator.producer().interval() != null
                                ? scheduledGenerator.producer().interval().toMillis()
                                : 0L;
                        final long nextTime = scheduledGenerator.startTime() + interval;
                        scheduler.schedule(scheduledGenerator.producer(), nextTime);
                    }
                }
            }
        } catch (Exception e) {
            setState(State.FAILED);
            throw new RunnerException("Unhandled producer exception", e);
        }
        setState(State.STOPPED);
        isRunning.set(false);
        log.info("Producer(s) stopped");
    }

    /**
     * Creates a Kafka producer based on the provided config.
     * This method is package protected so we can override it for testing
     *
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
    public synchronized State getState() {
        return currentState;
    }

    @Override
    public void stop() {
        setState(State.STOPPING);
        stopRunning.set(true);
    }
}
