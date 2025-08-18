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
import io.axual.ksml.client.resolving.ResolvingClientConfig;
import io.axual.ksml.generator.TopologyDefinition;
import io.axual.ksml.python.PythonContext;
import io.axual.ksml.python.PythonContextConfig;
import io.axual.ksml.python.PythonFunction;
import io.axual.ksml.runner.exception.RunnerException;
import io.axual.ksml.runner.producer.ExecutableProducer;
import io.axual.ksml.runner.producer.IntervalSchedule;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

@Slf4j
public class KafkaProducerRunner implements Runner {
    private static final String UNDEFINED = "undefined";
    private final IntervalSchedule scheduler = new IntervalSchedule();
    private final AtomicBoolean hasFailed = new AtomicBoolean(false);
    private final AtomicBoolean stopRunning = new AtomicBoolean(false);
    private final Config config;
    private final Function<Map<String, Object>, Producer<byte[], byte[]>> producerFactory;
    private State currentState;

    @Builder
    public record Config(
            Map<String, TopologyDefinition> definitions,
            Map<String, String> kafkaConfig,
            PythonContextConfig pythonContextConfig) {
        public Config(final Map<String, TopologyDefinition> definitions,
                      final Map<String, String> kafkaConfig,
                      final PythonContextConfig pythonContextConfig) {
            var processedKafkaConfig = new HashMap<>(kafkaConfig);
            this.definitions = definitions;
            // Check if a resolving client is required
            if (ResolvingClientConfig.configRequiresResolving(processedKafkaConfig)) {
                log.info("Using resolving Kafka clients");
                // Replace the deprecated configuration keys with the current ones
                ResolvingClientConfig.replaceDeprecatedConfigKeys(processedKafkaConfig);
            }
            this.kafkaConfig = processedKafkaConfig;
            this.pythonContextConfig = pythonContextConfig;
        }
    }

    public KafkaProducerRunner(Config config) {
        this.config = config;
        currentState = State.CREATED;
        this.producerFactory = ResolvingProducer::new;
    }

    // Package private to allow tests to inject configs
    KafkaProducerRunner(Config config, Function<Map<String, Object>, Producer<byte[], byte[]>> producerFactory) {
        this.config = config;
        currentState = State.CREATED;
        this.producerFactory = producerFactory;
    }

    private synchronized void setState(State newState) {
        if (currentState.isValidNextState(newState)) {
            currentState = newState;
        } else {
            log.warn("Illegal Producer State transition. Current state is {}. Invalid next state {}", currentState, newState);
        }
    }

    public void run() {
        log.info("Registering Kafka producer(s)");
        setState(State.STARTING);

        try {
            config.definitions.forEach((defName, definition) -> {
                // Log the start of the producer
                log.info("Starting producer definition: name={}, version={}, namespace={}",
                        definition.name() != null ? definition.name() : UNDEFINED,
                        definition.version() != null ? definition.version() : UNDEFINED,
                        definition.namespace() != null ? definition.namespace() : UNDEFINED);
                // Set up the Python context for this definition
                final var context = new PythonContext(config.pythonContextConfig());
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

        try (final Producer<byte[], byte[]> producer = producerFactory.apply(getProducerConfigs())) {
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
        log.info("Producer(s) stopped");
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
