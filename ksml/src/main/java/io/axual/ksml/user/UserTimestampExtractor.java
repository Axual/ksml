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


import io.axual.ksml.data.exception.ExecutionException;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.mapper.RecordContextDataObjectMapper;
import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataLong;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.tag.ContextTags;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.dsl.ConsumerRecordSchema;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.python.Invoker;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.RecordContext;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.TopicNameExtractor;

@Slf4j
public class UserTimestampExtractor extends Invoker implements TimestampExtractor {
    private final NativeDataObjectMapper nativeMapper = NativeDataObjectMapper.SUPPLIER().create();
    private final RecordContextDataObjectMapper recordContextMapper = new RecordContextDataObjectMapper();
    private final static DataType EXPECTED_RESULT_TYPE = DataString.DATATYPE;

    public UserTimestampExtractor(UserFunction function, ContextTags tags) {
        super(function, tags, KSMLDSL.Functions.TYPE_TIMESTAMPEXTRACTOR);
        verifyParameterCount(2);
        verifyResultType(EXPECTED_RESULT_TYPE);
    }

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
        final var dataRecord = new DataStruct(ConsumerRecordSchema.CONSUMER_RECORD_SCHEMA);
        dataRecord.put(ConsumerRecordSchema.CONSUMER_RECORD_SCHEMA_TIMESTAMP_FIELD, new DataLong(record.timestamp()));
        dataRecord.put(ConsumerRecordSchema.CONSUMER_RECORD_SCHEMA_TIMESTAMP_TYPE_FIELD, new DataString(record.timestampType().name));
        dataRecord.put(ConsumerRecordSchema.CONSUMER_RECORD_SCHEMA_KEY_FIELD, nativeMapper.toDataObject(record.key()));
        dataRecord.put(ConsumerRecordSchema.CONSUMER_RECORD_SCHEMA_VALUE_FIELD, nativeMapper.toDataObject(record.value()));
        dataRecord.put(ConsumerRecordSchema.CONSUMER_RECORD_SCHEMA_TOPIC_FIELD, new DataString(record.topic()));
        dataRecord.put(ConsumerRecordSchema.CONSUMER_RECORD_SCHEMA_PARTITION_FIELD, new DataInteger(record.partition()));
        dataRecord.put(ConsumerRecordSchema.CONSUMER_RECORD_SCHEMA_OFFSET_FIELD, new DataLong(record.offset()));
        final var result = timeExecutionOf(() -> function.call(dataRecord, new DataLong(previousTimestamp)));
        if (result instanceof DataLong dataLong) {
            return dataLong.value();
        }
        throw new ExecutionException("Expected long result from function: " + function.name);
    }
}
