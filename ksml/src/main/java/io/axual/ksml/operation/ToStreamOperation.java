package io.axual.ksml.operation;

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


import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Named;

import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.KTableWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.user.UserFunction;
import io.axual.ksml.user.UserKeyTransformer;
import io.axual.ksml.util.DataUtil;

public class ToStreamOperation extends BaseOperation {
    private static final String MAPPER_NAME = "Mapper";
    private final UserFunction mapper;

    public ToStreamOperation(OperationConfig config, UserFunction mapper) {
        super(config);
        this.mapper = mapper;
    }

    @Override
    public StreamWrapper apply(KTableWrapper input) {
        /*    Kafka Streams method signature:
         *    <KR> KStream<KR, V> toStream(
         *          final KeyValueMapper<? super K, ? super V, ? extends KR> mapper,
         *          final Named named)
         */

        var k = input.keyType().userType().dataType();
        var v = input.valueType().userType().dataType();
        var kr = mapper != null ? mapper.resultType.dataType() : k;
        if (mapper != null) checkFunction(MAPPER_NAME, mapper, equalTo(kr), superOf(k), superOf(v));

        KeyValueMapper<Object, Object, Object> userMapper = mapper != null
                ? new UserKeyTransformer(mapper)
                : (key, value) -> DataUtil.asDataObject(key);

        return new KStreamWrapper(
                input.table.toStream(userMapper, Named.as(name)),
                input.keyType(),
                input.valueType());
    }
}
