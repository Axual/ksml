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


import org.apache.kafka.streams.processor.RecordContext;
import org.apache.kafka.streams.processor.TopicNameExtractor;

import io.axual.ksml.data.object.DataString;
import io.axual.ksml.util.DataUtil;
import io.axual.ksml.exception.KSMLExecutionException;
import io.axual.ksml.python.Invoker;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UserTopicNameExtractor extends Invoker implements TopicNameExtractor<Object, Object> {
    public UserTopicNameExtractor(UserFunction function) {
        super(function);
        verifyParameterCount(2);
        verifyResultType(DataString.TYPE);
    }

    @Override
    public String extract(Object key, Object value, RecordContext recordContext) {
        var result = function.call(DataUtil.asData(key), DataUtil.asData(value));
        if (result instanceof DataString) {
            return ((DataString) result).value();
        }
        throw new KSMLExecutionException("Expected string result from function: " + function.name);
    }
}
