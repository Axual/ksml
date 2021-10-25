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
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;

import io.axual.ksml.data.object.user.UserLong;
import io.axual.ksml.data.type.user.StaticUserType;
import io.axual.ksml.data.type.user.UserType;
import io.axual.ksml.data.type.base.WindowedType;
import io.axual.ksml.stream.KGroupedStreamWrapper;
import io.axual.ksml.stream.KGroupedTableWrapper;
import io.axual.ksml.stream.KTableWrapper;
import io.axual.ksml.stream.SessionWindowedKStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.stream.TimeWindowedKStreamWrapper;

public class CountOperation extends StoreOperation {
    public CountOperation(StoreOperationConfig config) {
        super(config);
    }

    @Override
    public StreamWrapper apply(KGroupedStreamWrapper input) {
        return new KTableWrapper(
                (KTable) input.groupedStream.count(
                        Named.as(name),
                        registerStore(Materialized.as(storeName))),
                input.keyType(),
                streamDataTypeOf(new StaticUserType(UserLong.TYPE, UserType.DEFAULT_NOTATION), false));
    }

    @Override
    public StreamWrapper apply(KGroupedTableWrapper input) {
        return new KTableWrapper(
                (KTable) input.groupedTable.count(
                        Named.as(name),
                        registerStore(Materialized.as(storeName))),
                input.keyType(),
                streamDataTypeOf(new StaticUserType(UserLong.TYPE, UserType.DEFAULT_NOTATION), false));
    }

    @Override
    public StreamWrapper apply(SessionWindowedKStreamWrapper input) {
        return new KTableWrapper(
                (KTable) input.sessionWindowedKStream.count(
                        Named.as(name),
                        registerStore(Materialized.as(storeName))),
                streamDataTypeOf(new StaticUserType(new WindowedType(input.keyType.type()), input.keyType.notation().name()), true),
                streamDataTypeOf(new StaticUserType(UserLong.TYPE, UserType.DEFAULT_NOTATION), false));
    }

    @Override
    public StreamWrapper apply(TimeWindowedKStreamWrapper input) {
        return new KTableWrapper(
                (KTable) input.timeWindowedKStream.count(
                        Named.as(name),
                        registerStore(Materialized.as(storeName))),
                streamDataTypeOf(new StaticUserType(new WindowedType(input.keyType.type()), input.keyType.notation().name()), true),
                streamDataTypeOf(new StaticUserType(UserLong.TYPE, UserType.DEFAULT_NOTATION), false));
    }
}
