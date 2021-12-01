package io.axual.ksml.user;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 Axual B.V.
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


import org.apache.kafka.common.header.Header;
import org.apache.kafka.streams.processor.RecordContext;
import org.apache.kafka.streams.processor.TopicNameExtractor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import io.axual.ksml.data.object.base.Tuple;
import io.axual.ksml.data.object.user.UserString;
import io.axual.ksml.exception.KSMLExecutionException;
import io.axual.ksml.python.Invoker;
import io.axual.ksml.util.DataUtil;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UserTopicNameExtractor extends Invoker implements TopicNameExtractor<Object, Object> {
    private static final String HEADERS = "headers";
    private static final String OFFSET = "offset";
    private static final String PARTITION = "partition";
    private static final String TIMESTAMP = "timestamp";
    private static final String TOPIC = "topic";

    public UserTopicNameExtractor(UserFunction function) {
        super(function);
        verifyParameterCount(3);
        verifyResultType(UserString.TYPE);
    }

    @Override
    public String extract(Object key, Object value, RecordContext recordContext) {
        var result = function.call(
                DataUtil.asUserObject(key),
                DataUtil.asUserObject(value),
                DataUtil.asUserObject(convertRecordContext(recordContext)));
        if (result instanceof UserString) {
            return ((UserString) result).value();
        }
        throw new KSMLExecutionException("Expected string result from function: " + function.name);
    }

    private Map<String, Object> convertRecordContext(RecordContext recordContext) {
        final var result = new HashMap<String, Object>();
        result.put(OFFSET, recordContext.offset());
        result.put(TIMESTAMP, recordContext.timestamp());
        result.put(TOPIC, recordContext.topic());
        result.put(PARTITION, recordContext.partition());
        final var headers = new ArrayList<Tuple<Object>>();
        for (Header header : recordContext.headers()) {
            Tuple<Object> h = new Tuple<>(header.key(), header.value());
            headers.add(h);
        }
        result.put(HEADERS, headers);
        return result;
    }
}
