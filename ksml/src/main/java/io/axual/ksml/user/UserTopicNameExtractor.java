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
import io.axual.ksml.data.mapper.RecordContextDataObjectMapper;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.exception.ExecutionException;
import io.axual.ksml.metric.MetricTags;
import io.axual.ksml.python.Invoker;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.RecordContext;
import org.apache.kafka.streams.processor.TopicNameExtractor;

@Slf4j
public class UserTopicNameExtractor extends Invoker implements TopicNameExtractor<Object, Object> {
    public static final DataType EXPECTED_RESULT_TYPE = DataString.DATATYPE;
    private static final NativeDataObjectMapper NATIVE_MAPPER = new DataObjectFlattener();
    private static final RecordContextDataObjectMapper RECORD_CONTEXT_MAPPER = new RecordContextDataObjectMapper();

    public UserTopicNameExtractor(UserFunction function, MetricTags tags) {
        super(function, tags, KSMLDSL.Functions.TYPE_TOPICNAMEEXTRACTOR);
        verifyParameterCount(3);
        verifyResultType(EXPECTED_RESULT_TYPE);
    }

    @Override
    public String extract(Object key, Object value, RecordContext recordContext) {
        final var result = timeExecutionOf(() -> function.call(NATIVE_MAPPER.toDataObject(key), NATIVE_MAPPER.toDataObject(value), RECORD_CONTEXT_MAPPER.toDataObject(recordContext)));
        if (result instanceof DataString dataString) {
            return dataString.value();
        }
        throw new ExecutionException("Expected string result from function: " + function.name);
    }
}
