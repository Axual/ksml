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
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.metric.MetricTags;
import io.axual.ksml.python.Invoker;
import org.apache.kafka.streams.processor.StreamPartitioner;

import java.util.Optional;
import java.util.Set;

public class UserStreamPartitioner extends Invoker implements StreamPartitioner<Object, Object> {
    private static final DataType EXPECTED_RESULT_TYPE = DataInteger.DATATYPE;
    private static final NativeDataObjectMapper NATIVE_MAPPER = new DataObjectFlattener();

    public UserStreamPartitioner(UserFunction function, MetricTags tags) {
        super(function, tags, KSMLDSL.Functions.TYPE_STREAMPARTITIONER);
        verifyParameterCount(4);
        verifyResultType(EXPECTED_RESULT_TYPE);
    }

    @Override
    public Optional<Set<Integer>> partitions(String topic, Object key, Object value, int numPartitions) {
        final var result = timeExecutionOf(() -> function.call(new DataString(topic), NATIVE_MAPPER.toDataObject(key), NATIVE_MAPPER.toDataObject(value), new DataInteger(numPartitions)));
        if (result instanceof DataInteger dataInteger) {
            return Optional.of(Set.of(dataInteger.value()));
        }
        return Optional.empty();
    }
}