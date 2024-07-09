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

import io.axual.ksml.data.notation.UserType;
import io.axual.ksml.generator.TopologyBuildContext;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Named;

public class ConvertKeyValueOperation extends BaseOperation {
    private final UserType targetKeyType;
    private final UserType targetValueType;

    public ConvertKeyValueOperation(OperationConfig config, UserType targetKeyType, UserType targetValueType) {
        super(config);
        this.targetKeyType = targetKeyType;
        this.targetValueType = targetValueType;
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input, TopologyBuildContext context) {
        final var k = input.keyType();
        final var v = input.valueType();
        final var kr = streamDataTypeOf(targetKeyType, true).flatten();
        final var vr = streamDataTypeOf(targetValueType, false).flatten();
        final var mapper = context.getDataObjectConverter();

        // Set up the mapping function to convert the key and value
        KeyValueMapper<Object, Object, KeyValue<Object, Object>> converter = (key, value) -> {
            var keyAsData = flattenValue(key);
            var convertedKey = mapper.convert(k.userType().notation(), keyAsData, kr.userType());

            var valueAsData = flattenValue(value);
            var convertedValue = mapper.convert(v.userType().notation(), valueAsData, vr.userType());

            return new KeyValue<>(convertedKey, convertedValue);
        };

        final var output = name != null
                ? input.stream.map(converter, Named.as(name))
                : input.stream.map(converter);

        // Inject the mapper into the topology
        return new KStreamWrapper(output, kr, vr);
    }
}
