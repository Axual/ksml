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


import org.apache.kafka.streams.processor.StreamPartitioner;

import io.axual.ksml.python.Invoker;
import io.axual.ksml.type.StandardType;

public class UserStreamPartitioner extends Invoker implements StreamPartitioner<Object, Object> {

    public UserStreamPartitioner(UserFunction function) {
        super(function);
        verifyParameterCount(4);
        verifyResultType(StandardType.INTEGER);
    }

    @Override
    public Integer partition(String topic, Object key, Object value, int numPartitions) {
        return (Integer) function.call(topic, key, value, numPartitions);
    }
}
