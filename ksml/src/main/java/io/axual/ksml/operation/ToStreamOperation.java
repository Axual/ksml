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


import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.generator.TopologyBuildContext;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.KTableWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.user.UserKeyTransformer;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;

public class ToStreamOperation extends BaseOperation {
    private static final String MAPPER_NAME = "Mapper";
    private final FunctionDefinition mapper;

    public ToStreamOperation(OperationConfig config, FunctionDefinition mapper) {
        super(config);
        this.mapper = mapper;
    }

    @Override
    public StreamWrapper apply(KTableWrapper input, TopologyBuildContext context) {
        final var k = input.keyType();
        final var v = input.valueType();
        final var kr = mapper != null ? streamDataTypeOf(firstSpecificType(mapper, k), true) : k;
        final var map = mapper != null ? userFunctionOf(context, MAPPER_NAME, mapper, kr, superOf(k), superOf(v)) : null;
        final KeyValueMapper<Object, Object, Object> userMap = map != null
                ? new UserKeyTransformer(map, tags)
                : null;
        final var named = namedOf();
        final KStream<Object, Object> output = userMap != null
                ? named != null
                ? input.table.toStream(userMap, named)
                : input.table.toStream(userMap)
                : named != null
                ? input.table.toStream(named)
                : input.table.toStream();
        return new KStreamWrapper(output, kr, v);
    }
}
