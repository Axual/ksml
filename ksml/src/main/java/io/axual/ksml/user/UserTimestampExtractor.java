package io.axual.ksml.user;

/*-
 * ========================LICENSE_START=================================
 * KSML
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


import io.axual.ksml.data.mapper.DataObjectFlattener;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataLong;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.dsl.ConsumerRecordSchema;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.exception.ExecutionException;
import io.axual.ksml.metric.MetricTags;
import io.axual.ksml.python.Invoker;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

@Slf4j
public class UserTimestampExtractor extends Invoker implements TimestampExtractor {
    public static final DataType EXPECTED_RESULT_TYPE = DataLong.DATATYPE;
    private static final NativeDataObjectMapper NATIVE_MAPPER = new DataObjectFlattener();

    public UserTimestampExtractor(UserFunction function, MetricTags tags) {
        super(function, tags, KSMLDSL.Functions.TYPE_TIMESTAMPEXTRACTOR);
        verifyParameterCount(2);
        verifyResultType(EXPECTED_RESULT_TYPE);
    }

    @Override
    public long extract(ConsumerRecord<Object, Object> rec, long previousTimestamp) {
        final var dataRecord = new DataStruct(ConsumerRecordSchema.CONSUMER_RECORD_SCHEMA);
        dataRecord.put(ConsumerRecordSchema.CONSUMER_RECORD_SCHEMA_TIMESTAMP_FIELD, new DataLong(rec.timestamp()));
        dataRecord.put(ConsumerRecordSchema.CONSUMER_RECORD_SCHEMA_TIMESTAMP_TYPE_FIELD, new DataString(rec.timestampType().toString()));
        dataRecord.put(ConsumerRecordSchema.CONSUMER_RECORD_SCHEMA_KEY_FIELD, NATIVE_MAPPER.toDataObject(rec.key()));
        dataRecord.put(ConsumerRecordSchema.CONSUMER_RECORD_SCHEMA_VALUE_FIELD, NATIVE_MAPPER.toDataObject(rec.value()));
        dataRecord.put(ConsumerRecordSchema.CONSUMER_RECORD_SCHEMA_TOPIC_FIELD, new DataString(rec.topic()));
        dataRecord.put(ConsumerRecordSchema.CONSUMER_RECORD_SCHEMA_PARTITION_FIELD, new DataInteger(rec.partition()));
        dataRecord.put(ConsumerRecordSchema.CONSUMER_RECORD_SCHEMA_OFFSET_FIELD, new DataLong(rec.offset()));
        final var result = timeExecutionOf(() -> function.call(dataRecord, new DataLong(previousTimestamp)));
        if (result instanceof DataLong dataLong) {
            return dataLong.value();
        }
        throw new ExecutionException("Expected long result from function: " + function.name);
    }
}
