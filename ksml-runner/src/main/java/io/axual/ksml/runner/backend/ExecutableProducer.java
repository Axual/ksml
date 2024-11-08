package io.axual.ksml.runner.backend;

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

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serializer;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import io.axual.ksml.client.serde.ResolvingSerializer;
import io.axual.ksml.data.mapper.DataObjectConverter;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.notation.NotationLibrary;
import io.axual.ksml.data.notation.UserType;
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataTuple;
import io.axual.ksml.data.tag.ContextTags;
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.definition.ProducerDefinition;
import io.axual.ksml.python.PythonContext;
import io.axual.ksml.python.PythonFunction;
import io.axual.ksml.user.UserFunction;
import io.axual.ksml.user.UserGenerator;
import io.axual.ksml.user.UserPredicate;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import static io.axual.ksml.data.notation.UserType.DEFAULT_NOTATION;

@Slf4j
public class ExecutableProducer {
    private static final NativeDataObjectMapper NATIVE_MAPPER = new NativeDataObjectMapper();
    private static final DataObjectConverter DATA_OBJECT_CONVERTER = new DataObjectConverter();

    @Getter
    private final String name;
    private final UserGenerator generator;
    private final UserPredicate condition;
    private final String topic;
    private final UserType keyType;
    private final UserType valueType;
    private final Serializer<Object> keySerializer;
    private final Serializer<Object> valueSerializer;
    private final RescheduleStrategy rescheduleStrategy;

    private DataObject lastKey = DataNull.INSTANCE;
    private DataObject lastValue = DataNull.INSTANCE;

    private ExecutableProducer(UserFunction generator,
                               UserFunction condition,
                               ContextTags tags,
                               String topic,
                               UserType keyType,
                               UserType valueType,
                               Serializer<Object> keySerializer,
                               Serializer<Object> valueSerializer,
                               RescheduleStrategy rescheduleStrategy) {
        this.name = generator.name;
        this.generator = new UserGenerator(generator, tags);
        this.condition = condition != null ? new UserPredicate(condition, tags) : null;
        this.topic = topic;
        this.keyType = keyType;
        this.valueType = valueType;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.rescheduleStrategy = rescheduleStrategy;
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
        final var gen = producerDefinition.generator();
        final var tags = new ContextTags().append("namespace", namespace);
        final var generator = gen.name() != null
                ? PythonFunction.forGenerator(context, namespace, gen.name(), gen)
                : PythonFunction.forGenerator(context, namespace, name, gen);
        final var cond = producerDefinition.condition();
        final var condition = cond != null
                ? cond.name() != null
                ? PythonFunction.forPredicate(context, namespace, cond.name(), cond)
                : PythonFunction.forPredicate(context, namespace, name, cond)
                : null;
        final var keySerde = NotationLibrary.notation(target.keyType().notation()).serde(target.keyType().dataType(), true);
        final var keySerializer = new ResolvingSerializer<>(keySerde.serializer(), kafkaConfig);
        final var valueSerde = NotationLibrary.notation(target.valueType().notation()).serde(target.valueType().dataType(), false);
        final var valueSerializer = new ResolvingSerializer<>(valueSerde.serializer(), kafkaConfig);
        final var reschedulingStrategy = setupRescheduling(producerDefinition, context, namespace, name);

        return new ExecutableProducer(generator, condition, tags, target.topic(), target.keyType(), target.valueType(), keySerializer, valueSerializer, reschedulingStrategy);
    }

    public void produceMessage(Producer<byte[], byte[]> producer) {
        DataObject result = generator.apply();
        if (result instanceof DataTuple tuple && tuple.size() == 2) {
            var key = tuple.get(0);
            var value = tuple.get(1);

            if (condition == null || condition.test(key, value)) {
                // keep produced key and value to determine rescheduling later
                lastKey = key;
                lastValue = value;

                key = DATA_OBJECT_CONVERTER.convert(DEFAULT_NOTATION, key, keyType);
                value = DATA_OBJECT_CONVERTER.convert(DEFAULT_NOTATION, value, valueType);
                var okay = true;

                if (key != null && !keyType.dataType().isAssignableFrom(key.type())) {
                    log.error("Can not convert {} to topic key type {}", key.type(), keyType);
                    okay = false;
                }
                if (value != null && !valueType.dataType().isAssignableFrom(value.type())) {
                    log.error("Can not convert {} to topic value type {}", value.type(), valueType);
                    okay = false;
                }

                if (okay) {
                    var keyStr = key != null ? key.toString() : "null";
                    var valueStr = value != null ? value.toString() : "null";

                    keyStr = keyStr.replace("\n", "\\\\n");
                    valueStr = valueStr.replace("\n", "\\\\n");
                    log.info("Message: key={}, value={}", keyStr, valueStr);

                    // Headers are sometimes needed for Apicurio serialisation
                    var headers = new RecordHeaders();
                    var serializedKey = keySerializer.serialize(topic, headers, NATIVE_MAPPER.fromDataObject(key));
                    var serializedValue = valueSerializer.serialize(topic, headers, NATIVE_MAPPER.fromDataObject(value));
                    ProducerRecord<byte[], byte[]> message = new ProducerRecord<>(
                            topic,
                            null,
                            serializedKey,
                            serializedValue,
                            headers
                    );
                    var future = producer.send(message);
                    try {
                        var metadata = future.get();
                        if (metadata != null && metadata.hasOffset()) {
                            log.info("Produced message to {}, partition {}, offset {}", metadata.topic(), metadata.partition(), metadata.offset());
                        } else {
                            log.error("Error producing message to topic {}", topic);
                        }
                    } catch (InterruptedException | ExecutionException e) {
                        throw new io.axual.ksml.data.exception.ExecutionException("Could not produce to topic " + topic, e);
                    }
                }
            } else {
                log.info("Condition FALSE, skipping message");
            }
        }
    }

    /**
     * Indicate if this instance wants to be rescheduled after its most recent run.
     *
     * @return true if should reschedule.
     */
    public boolean shouldReschedule() {
        return rescheduleStrategy.shouldReschedule(lastKey, lastValue);
    }

    /**
     * Indicate the desired waiting time until the next reschedule.
     *
     * @return the desired wait until next run.
     */
    public Duration interval() {
        return rescheduleStrategy.interval();
    }

    private static RescheduleStrategy setupRescheduling(ProducerDefinition definition, PythonContext context, String namespace, String name) {
        if (definition.interval() == null) {
            return RescheduleStrategy.once();
        }

        AlwaysReschedule strategy = RescheduleStrategy.always(definition.interval());

        if (definition.count() != null) {
            // since the producer is always called first before calling the strategy, decrement count by 1
            strategy.combine(RescheduleStrategy.counting(definition.count() - 1));
        }

        if (definition.until() != null) {
            FunctionDefinition untilDefinition = definition.until();
            final var untilFunction = untilDefinition.name() != null
                    ? PythonFunction.forPredicate(context, namespace, untilDefinition.name(), untilDefinition)
                    : PythonFunction.forPredicate(context, namespace, name, untilDefinition);
            strategy.combine(RescheduleStrategy.until(untilFunction));
        }

        return strategy;
    }
}
