package io.axual.ksml.operation;

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

import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.ValueMapper;

import io.axual.ksml.data.mapper.DataObjectConverter;
import io.axual.ksml.data.type.UserType;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.util.DataUtil;

public class ConvertValueOperation extends BaseOperation {
    private final DataObjectConverter mapper;
    private final UserType targetValueType;

    public ConvertValueOperation(OperationConfig config, UserType targetValueType) {
        super(config);
        this.mapper = new DataObjectConverter(notationLibrary);
        this.targetValueType = targetValueType;
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input) {
        final var k = input.keyType();
        final var v = input.valueType();
        final var vr = streamDataTypeOf(targetValueType, false);

        // Set up the mapping function to convert the value
        ValueMapper<Object, Object> converter = value -> {
            var valueAsData = DataUtil.asDataObject(value);
            return mapper.convert(v.userType().notation(), valueAsData, vr.userType());
        };

        final var output = name != null
                ? input.stream.mapValues(converter, Named.as(name))
                : input.stream.mapValues(converter);

        // Inject the mapper into the topology
        return new KStreamWrapper(output, k, vr);
    }
}
