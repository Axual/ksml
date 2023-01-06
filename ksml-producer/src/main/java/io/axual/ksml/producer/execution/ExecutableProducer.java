package io.axual.ksml.producer.execution;

/*-
 * ========================LICENSE_START=================================
 * KSML Producer
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
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.object.DataTuple;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.StructType;
import io.axual.ksml.exception.KSMLExecutionException;
import io.axual.ksml.user.UserFunction;

public class ExecutableProducer {
    private static final Logger LOG = LoggerFactory.getLogger(ExecutableProducer.class);
    private final UserFunction generator;
    private final String topic;
    private final DataType keyType;
    private final DataType valueType;
    private final Serializer<Object> keySerializer;
    private final Serializer<Object> valueSerializer;
    private final NativeDataObjectMapper mapper = new NativeDataObjectMapper();

    public ExecutableProducer(UserFunction generator,
                              String topic,
                              DataType keyType,
                              DataType valueType,
                              Serializer<Object> keySerializer,
                              Serializer<Object> valueSerializer) {
        this.generator = generator;
        this.topic = topic;
        this.keyType = keyType;
        this.valueType = valueType;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }

    private DataObject tryToMakeCompatible(DataObject object, DataType type) {
        if (object instanceof DataStruct struct && type instanceof StructType structType) {
            var result = new DataStruct(structType);
            result.putAll(struct);
            return result;
        }
        return object;
    }

    public void produceMessage(Producer<byte[], byte[]> producer) {
        DataObject result = generator.call();
        if (result instanceof DataTuple tuple && tuple.size() == 2) {
            var key = tryToMakeCompatible(tuple.get(0), keyType);
            var value = tryToMakeCompatible(tuple.get(1), valueType);
            var okay = true;

            if (!keyType.isAssignableFrom(key.type())) {
                LOG.error("Can not convert {} to topic key type {}", key.type(), keyType);
                okay = false;
            }
            if (!valueType.isAssignableFrom(value.type())) {
                LOG.error("Can not convert {} to topic value type {}", value.type(), valueType);
                okay = false;
            }

            if (okay) {
                var keyStr = key != null ? key.toString() : "null";
                var valueStr = value != null ? value.toString() : "null";

                keyStr = keyStr.replaceAll("\n", "\\\\n");
                valueStr = valueStr.replaceAll("\n", "\\\\n");
                LOG.info("Message: key={}, value={}", keyStr, valueStr);

                var serializedKey = keySerializer.serialize(topic, mapper.fromDataObject(key));
                var serializedValue = valueSerializer.serialize(topic, mapper.fromDataObject(value));
                ProducerRecord<byte[], byte[]> message = new ProducerRecord<>(
                        topic,
                        serializedKey,
                        serializedValue
                );
                var future = producer.send(message);
                try {
                    var metadata = future.get();
                    if (metadata != null && metadata.hasOffset()) {
                        LOG.info("Produced message to {}, partition {}, offset {}", metadata.topic(), metadata.partition(), metadata.offset());
                    } else {
                        LOG.error("Error producing message to topic {}", topic);
                    }
                } catch (InterruptedException | ExecutionException e) {
                    throw new KSMLExecutionException("Could not produce to topic " + topic, e);
                }
            }
        }
    }
}
