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
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.notation.NotationLibrary;
import io.axual.ksml.data.notation.UserType;
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataTuple;
import io.axual.ksml.data.tag.ContextTags;
import io.axual.ksml.data.value.Pair;
import io.axual.ksml.definition.ProducerDefinition;
import io.axual.ksml.exception.TopologyException;
import io.axual.ksml.python.PythonContext;
import io.axual.ksml.python.PythonFunction;
import io.axual.ksml.user.UserFunction;
import io.axual.ksml.user.UserGenerator;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static io.axual.ksml.data.notation.UserType.DEFAULT_NOTATION;

@Slf4j
public class ExecutableProducer {
    private static final NativeDataObjectMapper NATIVE_MAPPER = new NativeDataObjectMapper();
    private static final DataObjectConverter DATA_OBJECT_CONVERTER = new DataObjectConverter();

    @Getter
    private final String name;
    private final UserGenerator generator;
    private final ProducerStrategy producerStrategy;
    private final String topic;
    private final UserType keyType;
    private final UserType valueType;
    private final Serializer<Object> keySerializer;
    private final Serializer<Object> valueSerializer;
    private long batchCount = 0;
    private boolean stopProducing = false;

    private ExecutableProducer(UserFunction generator,
                               ProducerStrategy producerStrategy,
                               ContextTags tags,
                               String topic,
                               UserType keyType,
                               UserType valueType,
                               Serializer<Object> keySerializer,
                               Serializer<Object> valueSerializer) {
        this.name = generator.name;
        this.generator = new UserGenerator(generator, tags);
        this.producerStrategy = producerStrategy;
        this.topic = topic;
        this.keyType = keyType;
        this.valueType = valueType;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
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
        final var tags = new ContextTags().append("namespace", namespace);

        // Initialize the message generator
        final var gen = producerDefinition.generator();
        if (gen == null) {
            throw new TopologyException("Missing generator function for producer \"" + name + "\"");
        }
        final var generator = gen.name() != null
                ? PythonFunction.forGenerator(context, namespace, gen.name(), gen)
                : PythonFunction.forGenerator(context, namespace, name, gen);

        // Initialize the producer strategy
        final var producerStrategy = new ProducerStrategy(context, namespace, name, tags, producerDefinition);

        // Initialize serializers
        final var keySerde = NotationLibrary.get(target.keyType().notation()).serde(target.keyType().dataType(), true);
        final var keySerializer = new ResolvingSerializer<>(keySerde.serializer(), kafkaConfig);
        final var valueSerde = NotationLibrary.get(target.valueType().notation()).serde(target.valueType().dataType(), false);
        final var valueSerializer = new ResolvingSerializer<>(valueSerde.serializer(), kafkaConfig);

        // Set up the producer
        return new ExecutableProducer(generator, producerStrategy, tags, target.topic(), target.keyType(), target.valueType(), keySerializer, valueSerializer);
    }

    public void produceMessages(Producer<byte[], byte[]> producer) {
        final var messages = generateBatch();
        final var futures = new ArrayList<Future<RecordMetadata>>();
        try {
            for (var message : messages) {
                ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                        topic,
                        message.left(),
                        message.right()
                );
                futures.add(producer.send(record));
            }

            batchCount++;

            for (var future : futures) {
                var metadata = future.get();
                if (metadata != null && metadata.hasOffset()) {
                    producerStrategy.successfullyProducedOneMessage();
                    log.info("Produced message: producer={}, batch #{}, message #{}, topic={}, partition={}, offset={}", name, batchCount, producerStrategy.messagesProduced(), metadata.topic(), metadata.partition(), metadata.offset());
                } else {
                    log.error("Error producing message to topic {}", topic);
                }
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new io.axual.ksml.data.exception.ExecutionException("Could not produce to topic " + topic, e);
        }
    }

    private List<Pair<byte[], byte[]>> generateBatch() {
        final var result = new ArrayList<Pair<byte[], byte[]>>();
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
                var serializedKey = keySerializer.serialize(topic, NATIVE_MAPPER.fromDataObject(key));
                var serializedValue = valueSerializer.serialize(topic, NATIVE_MAPPER.fromDataObject(value));

                // Add the serialized message to the batch
                result.add(new Pair<>(serializedKey, serializedValue));

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
        DataObject result = generator.apply();
        if (result instanceof DataTuple tuple && tuple.size() == 2) {
            var key = tuple.get(0);
            var value = tuple.get(1);

            if (producerStrategy.validateMessage(key, value)) {
                // keep produced key and value to determine rescheduling later
                key = DATA_OBJECT_CONVERTER.convert(DEFAULT_NOTATION, key, keyType);
                value = DATA_OBJECT_CONVERTER.convert(DEFAULT_NOTATION, value, valueType);

                var okay = true;

                if (key != null && !keyType.dataType().isAssignableFrom(key.type())) {
                    log.error("Wrong topic key type: expected={} key={}", keyType, key.type());
                    okay = false;
                }
                if (value != null && !valueType.dataType().isAssignableFrom(value.type())) {
                    log.error("Wrong topic value type: expected={} value={}", valueType, value.type());
                    okay = false;
                }

                if (okay) {
                    return new Pair<>(key, value);
                }
            } else {
                log.warn("Skipping invalid message: key={} value={}", key, value);
            }
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
