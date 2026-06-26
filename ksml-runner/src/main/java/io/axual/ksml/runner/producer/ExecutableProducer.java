package io.axual.ksml.runner.producer;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Generator
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

import io.axual.ksml.client.serde.ResolvingSerializer;
import io.axual.ksml.data.mapper.DataObjectConverter;
import io.axual.ksml.data.object.DataList;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataTuple;
import io.axual.ksml.definition.ProducerDefinition;
import io.axual.ksml.exception.TopologyException;
import io.axual.ksml.execution.ExecutionContext;
import io.axual.ksml.metric.MetricTags;
import io.axual.ksml.python.PythonContext;
import io.axual.ksml.python.PythonFunction;
import io.axual.ksml.type.UserType;
import io.axual.ksml.user.UserFunction;
import io.axual.ksml.user.UserGenerator;
import io.axual.ksml.user.UserStreamPartitioner;
import io.axual.ksml.util.Pair;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static io.axual.ksml.type.UserType.DEFAULT_NOTATION;

@Slf4j
public class ExecutableProducer {
    private static final DataObjectConverter DATA_OBJECT_CONVERTER = new DataObjectConverter();

    @Getter
    private final String name;
    private final UserGenerator generator;
    private final ProducerStrategy producerStrategy;
    private final String topic;
    private final UserType keyType;
    private final UserType valueType;
    private final UserStreamPartitioner partitioner;
    private final Serializer<Object> keySerializer;
    private final Serializer<Object> valueSerializer;
    private long batchCount = 0;
    private boolean stopProducing = false;
    private final List<Pair<DataObject, DataObject>> messageQueue = new LinkedList<>();

    // Package-private (instead of private) so tests can construct a producer with a stubbed generator
    // and serializers, exercising the produce/batch logic without a Python/GraalVM runtime.
    ExecutableProducer(UserFunction generator,
                       ProducerStrategy producerStrategy,
                       MetricTags tags,
                       UserFunction partitioner,
                       ProducerTarget target) {
        this.name = generator.name;
        this.generator = new UserGenerator(generator, tags);
        this.producerStrategy = producerStrategy;
        this.topic = target.topic();
        this.keyType = target.keyType();
        this.valueType = target.valueType();
        this.partitioner = partitioner != null ? new UserStreamPartitioner(partitioner, tags) : null;
        this.keySerializer = target.keySerializer();
        this.valueSerializer = target.valueSerializer();
    }

    /**
     * Groups the destination topic together with the key/value types and their serializers. Keeping
     * these related values in one parameter object keeps the {@link ExecutableProducer} constructor
     * below the parameter-count threshold and documents that they describe a single producer target.
     */
    record ProducerTarget(String topic,
                          UserType keyType,
                          UserType valueType,
                          Serializer<Object> keySerializer,
                          Serializer<Object> valueSerializer) {
    }

    @SuppressWarnings("java:S6218")
    private record GeneratedMessage(Headers headers, byte[] key, byte[] value) {
    }

    /**
     * Return a new instance based on the givan parameters.
     *
     * @param context            the {@link PythonContext}.
     * @param namespace          the namespace for the function.
     * @param name               the name for the function definition.
     * @param producerDefinition the {@link ProducerDefinition} for this producer.
     * @param kafkaConfig        the Kafka configuration for this producer.
     * @return a new ExecutableProducer instance.
     */
    public static ExecutableProducer forProducer(PythonContext context, String namespace, String name, ProducerDefinition producerDefinition, Map<String, String> kafkaConfig) {
        final var target = producerDefinition.target();
        final var tags = new MetricTags().append("namespace", namespace);

        // Initialize the message generator
        final var gen = producerDefinition.generator();
        if (gen == null) {
            throw new TopologyException("Missing generator function for producer \"" + name + "\"");
        }
        final var generator = gen.name() != null
                ? PythonFunction.forGenerator(context, namespace, gen.name(), gen)
                : PythonFunction.forGenerator(context, namespace, name, gen);
        final var partitioner = target.partitioner() != null
                ? PythonFunction.forFunction(context, namespace, target.partitioner().name(), target.partitioner())
                : null;

        // Initialize the producer strategy
        final var producerStrategy = new ProducerStrategy(context, namespace, name, tags, producerDefinition);

        // Initialize serializers
        final var keyNotation = ExecutionContext.INSTANCE.notationLibrary().get(target.keyType().notation());
        final var keySerde = keyNotation.serde(target.keyType().dataType(), true);
        final var keySerializer = new ResolvingSerializer<>(keySerde.serializer(), kafkaConfig);
        final var valueNotation = ExecutionContext.INSTANCE.notationLibrary().get(target.valueType().notation());
        final var valueSerde = valueNotation.serde(target.valueType().dataType(), false);
        final var valueSerializer = new ResolvingSerializer<>(valueSerde.serializer(), kafkaConfig);

        // Set up the producer
        final var producerTarget = new ProducerTarget(target.topic(), target.keyType(), target.valueType(), keySerializer, valueSerializer);
        return new ExecutableProducer(generator, producerStrategy, tags, partitioner, producerTarget);
    }

    public void produceMessages(Producer<byte[], byte[]> producer) {
        final var messages = generateBatch();
        final var futures = new ArrayList<Future<RecordMetadata>>();
        try {
            for (final var message : messages) {
                if (partitioner != null) {
                    // If a partitioner is defined, then call the function and generate producer records for every
                    // partition the message is sent to
                    final var numPartitions = producer.partitionsFor(topic).size();
                    Optional<Set<Integer>> partitions = partitioner.partitions(topic, message.key(), message.value(), numPartitions);
                    if (partitions.isPresent()) {
                        for (int partition : partitions.get()) {
                            ProducerRecord<byte[], byte[]> rec = new ProducerRecord<>(topic, partition, message.key(), message.value(), message.headers());
                            futures.add(producer.send(rec));
                        }
                    }
                } else {
                    // No partitioner is defined, so create just one producer record without specifying a partition
                    ProducerRecord<byte[], byte[]> rec = new ProducerRecord<>(topic, null, message.key(), message.value(), message.headers());
                    futures.add(producer.send(rec));
                }
            }

            batchCount++;

            for (var future : futures) {
                final var metadata = future.get();
                if (metadata != null && metadata.hasOffset()) {
                    producerStrategy.successfullyProducedOneMessage();
                    log.info("Produced message: producer={}, batch #{}, message #{}, topic={}, partition={}, offset={}", name, batchCount, producerStrategy.messagesProduced(), metadata.topic(), metadata.partition(), metadata.offset());
                } else {
                    log.error("Error producing message to topic {}", topic);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new io.axual.ksml.exception.ExecutionException("Could not produce to topic " + topic, e);
        } catch (ExecutionException e) {
            throw new io.axual.ksml.exception.ExecutionException("Could not produce to topic " + topic, e);
        }
    }

    private List<GeneratedMessage> generateBatch() {
        final var result = new ArrayList<GeneratedMessage>();
        for (int index = 0; index < producerStrategy.batchSize(); index++) {
            Pair<DataObject, DataObject> message = null;
            for (int t = 0; t < 10; t++) {
                message = generateMessage();
                if (message != null) break;
            }

            if (message != null) {
                final var key = message.left();
                final var value = message.right();

                // Log the generated messages
                final var keyStr = key != null ? key.toString(DataObject.Printer.EXTERNAL_TOP_SCHEMA).replace("\n", "\\\\n") : "null";
                final var valueStr = value != null ? value.toString(DataObject.Printer.EXTERNAL_TOP_SCHEMA).replace("\n", "\\\\n") : "null";
                log.info("Message: key={}, value={}", keyStr, valueStr);

                // Serialize the message
                final var headers = new RecordHeaders();
                final var serializedKey = keySerializer.serialize(topic, headers, key);
                final var serializedValue = valueSerializer.serialize(topic, headers, value);

                // Add the serialized message to the batch
                result.add(new GeneratedMessage(headers, serializedKey, serializedValue));

                // Check if this should be the last message produced
                if (!producerStrategy.continueAfterMessage(key, value)) {
                    stopProducing = true;
                    break;
                }
            } else {
                log.warn("Could not generate a valid message after 10 tries, skipping...");
            }
        }

        // Return the batch of messages
        return result;
    }

    private Pair<DataObject, DataObject> generateMessage() {
        // Get a message for the queue of generated messages, or generate new messages first and then fetch
        // from the queue
        if (messageQueue.isEmpty()) messageQueue.addAll(generateMessages());
        // Return the first element of the queue, or null if none present
        return messageQueue.isEmpty() ? null : messageQueue.removeFirst();
    }

    private List<Pair<DataObject, DataObject>> generateMessages() {
        return extractMessages(generator.apply(), keyType, valueType, producerStrategy);
    }

    /**
     * Turns a generated {@link DataObject} into zero or more validated key/value messages. A single
     * (key, value) tuple yields one message, while a {@link DataList} yields one message per valid
     * tuple element; anything else is skipped. Extracted as a package-private static method for
     * testability.
     *
     * @param generated        the object returned by the generator function
     * @param keyType          the expected key type
     * @param valueType        the expected value type
     * @param producerStrategy the strategy used to validate and shape each message
     * @return the list of validated, type-checked messages (possibly empty)
     */
    static List<Pair<DataObject, DataObject>> extractMessages(DataObject generated, UserType keyType, UserType valueType, ProducerStrategy producerStrategy) {
        final var result = new ArrayList<Pair<DataObject, DataObject>>();
        if (generated instanceof DataTuple tuple && tuple.elements().size() == 2) {
            addShapedMessage(result, tuple, keyType, valueType, producerStrategy);
        }
        if (generated instanceof DataList list) {
            for (final var element : list) {
                if (element instanceof DataTuple tuple && tuple.elements().size() == 2) {
                    addShapedMessage(result, tuple, keyType, valueType, producerStrategy);
                } else {
                    log.warn("Skipping invalid message: {}", element);
                }
            }
        }
        return result;
    }

    private static void addShapedMessage(List<Pair<DataObject, DataObject>> result, DataTuple tuple, UserType keyType, UserType valueType, ProducerStrategy producerStrategy) {
        final var msg = shapeMessage(tuple, keyType, valueType, producerStrategy);
        if (msg != null) result.add(msg);
    }

    /**
     * Validates a (key, value) tuple against the producer strategy, converts both to the target types
     * and verifies they are assignable to the configured key/value types. Returns {@code null} when the
     * message is rejected by the strategy or has the wrong type. Extracted as a package-private static
     * method for testability.
     *
     * @param tuple            the generated (key, value) tuple
     * @param keyType          the expected key type
     * @param valueType        the expected value type
     * @param producerStrategy the strategy used to validate the message
     * @return the shaped (key, value) pair, or {@code null} when the message should be skipped
     */
    static Pair<DataObject, DataObject> shapeMessage(DataTuple tuple, UserType keyType, UserType valueType, ProducerStrategy producerStrategy) {
        var key = tuple.elements().get(0);
        var value = tuple.elements().get(1);

        if (producerStrategy.validateMessage(key, value)) {
            // keep produced key and value to determine rescheduling later
            key = DATA_OBJECT_CONVERTER.convert(DEFAULT_NOTATION, key, keyType);
            value = DATA_OBJECT_CONVERTER.convert(DEFAULT_NOTATION, value, valueType);

            var okay = true;

            if (key != null && keyType.dataType().isAssignableFrom(key.type()).isNotAssignable()) {
                log.error("Wrong topic key type: expected={} key={}", keyType, key.type());
                okay = false;
            }
            if (value != null && valueType.dataType().isAssignableFrom(value.type()).isNotAssignable()) {
                log.error("Wrong topic value type: expected={} value={}", valueType, value.type());
                okay = false;
            }

            if (okay) {
                return new Pair<>(key, value);
            }
        } else {
            log.warn("Skipping invalid message: key={} value={}", key, value);
        }
        return null;
    }

    /**
     * Indicate if this producer wants to be rescheduled after its most recent run.
     *
     * @return true if should reschedule.
     */
    public boolean shouldReschedule() {
        return !stopProducing && producerStrategy.shouldReschedule();
    }

    /**
     * Indicate the desired waiting time until the next reschedule.
     *
     * @return the desired wait until next run.
     */
    public Duration interval() {
        return producerStrategy.interval();
    }
}
