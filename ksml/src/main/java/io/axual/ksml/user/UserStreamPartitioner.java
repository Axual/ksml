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


import io.axual.ksml.data.type.DataType;
import org.apache.kafka.streams.processor.StreamPartitioner;

import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.util.DataUtil;
import io.axual.ksml.exception.KSMLExecutionException;
import io.axual.ksml.python.Invoker;

public class UserStreamPartitioner extends Invoker implements StreamPartitioner<Object, Object> {
    private final static DataType EXPECTED_RESULT_TYPE = DataInteger.DATATYPE;

    public UserStreamPartitioner(UserFunction function) {
        super(function);
        verifyParameterCount(4);
        verifyResultType(EXPECTED_RESULT_TYPE);
    }

    @Override
    public Integer partition(String topic, Object key, Object value, int numPartitions) {
        verifyAppliedResultType(EXPECTED_RESULT_TYPE);
        final var result = function.call(new DataString(topic), DataUtil.asDataObject(key), DataUtil.asDataObject(value), new DataInteger(numPartitions));
        if (result instanceof DataInteger dataInteger) {
            return dataInteger.value();
        }
        throw new KSMLExecutionException("Expected integer result from partitioner function: " + function.name);
    }
}
