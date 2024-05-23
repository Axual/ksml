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
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.tag.ContextTags;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.python.Invoker;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.RecordContext;
import org.apache.kafka.streams.processor.TopicNameExtractor;

@Slf4j
public class UserTopicNameExtractor extends Invoker implements TopicNameExtractor<Object, Object> {
    private final NativeDataObjectMapper nativeMapper = NativeDataObjectMapper.SUPPLIER().create();
    private final RecordContextDataObjectMapper recordContextMapper = new RecordContextDataObjectMapper();
    private final static DataType EXPECTED_RESULT_TYPE = DataString.DATATYPE;

    public UserTopicNameExtractor(UserFunction function, ContextTags tags) {
        super(function, tags, KSMLDSL.Functions.TYPE_TOPICNAMEEXTRACTOR);
        verifyParameterCount(3);
        verifyResultType(EXPECTED_RESULT_TYPE);
    }

    @Override
    public String extract(Object key, Object value, RecordContext recordContext) {
        final var result = function.call(nativeMapper.toDataObject(key), nativeMapper.toDataObject(value), recordContextMapper.toDataObject(recordContext));
        if (result instanceof DataString dataString) {
            return dataString.value();
        }
        throw new ExecutionException("Expected string result from function: " + function.name);
    }
}
