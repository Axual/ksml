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

import io.axual.ksml.data.type.base.DataType;
import io.axual.ksml.definition.BaseStreamDefinition;
import io.axual.ksml.exception.KSMLTypeException;
import io.axual.ksml.notation.NotationLibrary;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;

public class ToOperation extends BaseOperation {
    private final BaseStreamDefinition target;
    private final NotationLibrary notationLibrary;

    public ToOperation(OperationConfig config, BaseStreamDefinition target, NotationLibrary notationLibrary) {
        super(config);
        this.target = target;
        this.notationLibrary = notationLibrary;
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input) {
        // Perform a type check to see if the key/value data types received matches the stream definition's types
        if (!target.keyType.type().isAssignableFrom(input.keyType().type()) || !target.valueType.type().isAssignableFrom(input.valueType().type())) {
            throw KSMLTypeException.topicTypeMismatch(target.topic, input.keyType(), input.valueType(), target.keyType.type(), target.valueType.type());
        }

        var keySerde = target.keyType.type() != DataType.UNKNOWN
                ? notationLibrary.get(target.keyType.notation()).getSerde(target.keyType.type(), true)
                : input.keyType().getSerde();
        var valueSerde = target.valueType.type() != DataType.UNKNOWN
                ? notationLibrary.get(target.valueType.notation()).getSerde(target.valueType.type(), false)
                : input.valueType().getSerde();

        input.stream.to(target.topic, Produced.with(keySerde, valueSerde).withName(name));
        return null;
    }
}
