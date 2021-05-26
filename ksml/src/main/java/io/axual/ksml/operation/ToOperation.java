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


import org.apache.kafka.streams.kstream.Produced;

import io.axual.ksml.definition.BaseStreamDefinition;
import io.axual.ksml.exception.KSMLApplyException;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;

public class ToOperation extends BaseOperation {
    private final BaseStreamDefinition target;

    public ToOperation(String name, BaseStreamDefinition target) {
        super(name);
        this.target = target;
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input) {
        // Perform a type check to see if the key/value data types received matches the stream definition's types
        if (!target.keyType.isAssignableFrom(input.keyType.type) || !target.valueType.isAssignableFrom(input.valueType.type)) {
            throw new KSMLApplyException("Incompatible key/value types: " +
                    "topic=" + target.topic +
                    ", keyType=" + input.keyType +
                    ", valueType=" + input.valueType +
                    ", expected keyType=" + target.keyType +
                    ", expected valueType=" + target.valueType);
        }
        input.stream.to(target.topic, Produced.with(input.keyType.serde, input.valueType.serde).withName(name));
        return null;
    }
}
