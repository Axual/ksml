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


import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;

import io.axual.ksml.stream.KGroupedStreamWrapper;
import io.axual.ksml.stream.KGroupedTableWrapper;
import io.axual.ksml.stream.KTableWrapper;
import io.axual.ksml.stream.SessionWindowedKStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.stream.TimeWindowedKStreamWrapper;
import io.axual.ksml.user.UserFunction;
import io.axual.ksml.user.UserReducer;

public class ReduceOperation extends StoreOperation {
    private final UserFunction reducer;
    private final UserFunction adder;
    private final UserFunction subtractor;

    public ReduceOperation(StoreOperationConfig config, UserFunction reducer, UserFunction adder, UserFunction subtractor) {
        super(config);
        this.reducer = reducer;
        this.adder = adder;
        this.subtractor = subtractor;
    }

    @Override
    public StreamWrapper apply(KGroupedStreamWrapper input) {
        return new KTableWrapper(
                input.groupedStream.reduce(
                        new UserReducer(reducer),
                        Named.as(name),
                        registerKeyValueStore(storeName, input.keyType, input.valueType)),
                input.keyType(),
                input.valueType());
    }

    @Override
    public StreamWrapper apply(KGroupedTableWrapper input) {
        return new KTableWrapper(
                input.groupedTable.reduce(
                        new UserReducer(adder),
                        new UserReducer(subtractor),
                        Named.as(name),
                        registerKeyValueStore(storeName, input.keyType, input.valueType)),
                input.keyType(),
                input.valueType());
    }

    @Override
    public StreamWrapper apply(SessionWindowedKStreamWrapper input) {
        return new KTableWrapper(
                (KTable) input.sessionWindowedKStream.reduce(
                        new UserReducer(reducer),
                        Named.as(name),
                        registerSessionStore(storeName, input.keyType, input.valueType)),
                windowedTypeOf(input.keyType),
                input.valueType());
    }

    @Override
    public StreamWrapper apply(TimeWindowedKStreamWrapper input) {
        return new KTableWrapper(
                (KTable) input.timeWindowedKStream.reduce(
                        new UserReducer(reducer),
                        Named.as(name),
                        registerWindowStore(storeName, input.keyType, input.valueType)),
                windowedTypeOf(input.keyType),
                input.valueType());
    }
}
