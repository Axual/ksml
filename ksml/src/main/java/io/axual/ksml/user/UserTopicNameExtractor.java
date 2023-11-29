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


import io.axual.ksml.data.type.DataType;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.streams.processor.RecordContext;
import org.apache.kafka.streams.processor.TopicNameExtractor;

import io.axual.ksml.data.object.DataBytes;
import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataList;
import io.axual.ksml.data.object.DataLong;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.type.StructType;
import io.axual.ksml.exception.KSMLExecutionException;
import io.axual.ksml.python.Invoker;
import io.axual.ksml.util.DataUtil;
import lombok.extern.slf4j.Slf4j;

import static io.axual.ksml.dsl.RecordContextSchema.RECORD_CONTEXT_SCHEMA_HEADERS_FIELD;
import static io.axual.ksml.dsl.RecordContextSchema.RECORD_CONTEXT_HEADER_SCHEMA;
import static io.axual.ksml.dsl.RecordContextSchema.RECORD_CONTEXT_HEADER_SCHEMA_KEY_FIELD;
import static io.axual.ksml.dsl.RecordContextSchema.RECORD_CONTEXT_SCHEMA_OFFSET_FIELD;
import static io.axual.ksml.dsl.RecordContextSchema.RECORD_CONTEXT_SCHEMA_PARTITION_FIELD;
import static io.axual.ksml.dsl.RecordContextSchema.RECORD_CONTEXT_SCHEMA;
import static io.axual.ksml.dsl.RecordContextSchema.RECORD_CONTEXT_SCHEMA_TIMESTAMP_FIELD;
import static io.axual.ksml.dsl.RecordContextSchema.RECORD_CONTEXT_SCHEMA_TOPIC_FIELD;
import static io.axual.ksml.dsl.RecordContextSchema.RECORD_CONTEXT_HEADER_SCHEMA_VALUE_FIELD;

@Slf4j
public class UserTopicNameExtractor extends Invoker implements TopicNameExtractor<Object, Object> {
    private final static DataType EXPECTED_RESULT_TYPE = DataString.DATATYPE;

    public UserTopicNameExtractor(UserFunction function) {
        super(function);
        verifyParameterCount(3);
        verifyResultType(EXPECTED_RESULT_TYPE);
    }

    @Override
    public String extract(Object key, Object value, RecordContext recordContext) {
        final var result = function.call(
                DataUtil.asDataObject(key),
                DataUtil.asDataObject(value),
                convertRecordContext(recordContext));
        if (result instanceof DataString dataString) {
            return dataString.value();
        }
        throw new KSMLExecutionException("Expected string result from function: " + function.name);
    }

    private DataStruct convertRecordContext(RecordContext recordContext) {
        final var result = new DataStruct(RECORD_CONTEXT_SCHEMA);
        result.put(RECORD_CONTEXT_SCHEMA_OFFSET_FIELD, new DataLong(recordContext.offset()));
        result.put(RECORD_CONTEXT_SCHEMA_TIMESTAMP_FIELD, new DataLong(recordContext.timestamp()));
        result.put(RECORD_CONTEXT_SCHEMA_TOPIC_FIELD, new DataString(recordContext.topic()));
        result.put(RECORD_CONTEXT_SCHEMA_PARTITION_FIELD, new DataInteger(recordContext.partition()));
        final var headerList = new DataList(new StructType(RECORD_CONTEXT_HEADER_SCHEMA));
        for (Header header : recordContext.headers()) {
            var hdr = new DataStruct(RECORD_CONTEXT_HEADER_SCHEMA);
            hdr.put(RECORD_CONTEXT_HEADER_SCHEMA_KEY_FIELD, new DataString(header.key()));
            hdr.put(RECORD_CONTEXT_HEADER_SCHEMA_VALUE_FIELD, new DataBytes(header.value()));
            headerList.add(hdr);
        }
        result.put(RECORD_CONTEXT_SCHEMA_HEADERS_FIELD, headerList);
        return result;
    }
}
