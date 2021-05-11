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



import org.apache.kafka.streams.kstream.Repartitioned;

import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.user.UserFunction;
import io.axual.ksml.user.UserStreamPartitioner;

public class RepartitionOperation extends BaseOperation {
    private final String name;
    private final UserFunction partitioner;

    public RepartitionOperation(String name, UserFunction partitioner) {
        this.name = name;
        this.partitioner = partitioner;
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input) {
        Repartitioned<Object, Object> repartitioned = Repartitioned.with(input.keyType.serde, input.valueType.serde).withName(name);
        if (partitioner != null) {
            repartitioned = repartitioned.withStreamPartitioner(new UserStreamPartitioner(partitioner));
        }
        return new KStreamWrapper(
                input.stream.repartition(repartitioned),
                input.keyType,
                input.valueType);
    }
}
