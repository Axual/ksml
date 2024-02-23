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

import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.notation.UserType;
import io.axual.ksml.data.object.DataBoolean;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataTuple;
import io.axual.ksml.data.mapper.DataObjectConverter;
import io.axual.ksml.exception.KSMLExecutionException;
import io.axual.ksml.execution.FatalError;
import io.axual.ksml.user.UserFunction;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;

import java.util.concurrent.ExecutionException;

import static io.axual.ksml.data.notation.UserType.DEFAULT_NOTATION;

@Slf4j
public class ExecutableProducer {
    private final UserFunction generator;
    private final UserFunction condition;
    private final String topic;
    private final UserType keyType;
    private final UserType valueType;
    private final Serializer<Object> keySerializer;
    private final Serializer<Object> valueSerializer;
    private final NativeDataObjectMapper nativeMapper = NativeDataObjectMapper.SUPPLIER().create();
    private final DataObjectConverter dataObjectConverter;

    public ExecutableProducer(UserFunction generator,
                              UserFunction condition,
                              String topic,
                              UserType keyType,
                              UserType valueType,
                              Serializer<Object> keySerializer,
                              Serializer<Object> valueSerializer) {
        this.dataObjectConverter = new DataObjectConverter();
        this.generator = generator;
        this.condition = condition;
        this.topic = topic;
        this.keyType = keyType;
        this.valueType = valueType;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }

    public String name() {
        return generator.name;
    }

    public void produceMessage(Producer<byte[], byte[]> producer) {
        DataObject result = generator.call();
        if (result instanceof DataTuple tuple && tuple.size() == 2) {
            var key = tuple.get(0);
            var value = tuple.get(1);

            if (checkCondition(key, value)) {
                key = dataObjectConverter.convert(DEFAULT_NOTATION, key, keyType);
                value = dataObjectConverter.convert(DEFAULT_NOTATION, value, valueType);
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

                    keyStr = keyStr.replaceAll("\n", "\\\\n");
                    valueStr = valueStr.replaceAll("\n", "\\\\n");
                    log.info("Message: key={}, value={}", keyStr, valueStr);

                    var serializedKey = keySerializer.serialize(topic, nativeMapper.fromDataObject(key));
                    var serializedValue = valueSerializer.serialize(topic, nativeMapper.fromDataObject(value));
                    ProducerRecord<byte[], byte[]> message = new ProducerRecord<>(
                            topic,
                            serializedKey,
                            serializedValue
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
                        throw new KSMLExecutionException("Could not produce to topic " + topic, e);
                    }
                }
            } else {
                log.info("Condition FALSE, skipping message");
            }
        }
    }

    private boolean checkCondition(DataObject key, DataObject value) {
        if (condition == null) return true;
        DataObject result = condition.call(key, value);
        if (result instanceof DataBoolean resultBoolean) return resultBoolean.value();
        throw FatalError.executionError("Producer condition did not return a boolean value: " + condition.name);
    }
}
