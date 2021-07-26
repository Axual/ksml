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


import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.StreamJoined;

import java.time.Duration;

import io.axual.ksml.exception.KSMLApplyException;
import io.axual.ksml.generator.StreamDataType;
import io.axual.ksml.stream.BaseStreamWrapper;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.KTableWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.user.UserFunction;
import io.axual.ksml.user.UserValueJoiner;

public class OuterJoinOperation extends StoreOperation {
    private final BaseStreamWrapper joinStream;
    private final UserFunction valueJoiner;
    private final JoinWindows joinWindows;

    public OuterJoinOperation(String name, String storeName, KStreamWrapper joinStream, UserFunction valueJoiner, Duration joinWindowDuration) {
        super(name, storeName);
        this.joinStream = joinStream;
        this.valueJoiner = valueJoiner;
        this.joinWindows = JoinWindows.of(joinWindowDuration);
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input) {
        final StreamDataType resultValueType = StreamDataType.of(valueJoiner.resultType, input.valueType.notation, false);

        if (joinStream instanceof KStreamWrapper) {
            return new KStreamWrapper(
                    input.stream.outerJoin(
                            ((KStreamWrapper) joinStream).stream,
                            new UserValueJoiner(valueJoiner),
                            joinWindows,
                            StreamJoined.with(input.keyType.getSerde(), input.valueType.getSerde(), resultValueType.getSerde()).withName(name).withStoreName(storeName)),
                    input.keyType,
                    resultValueType);
        }
        throw new KSMLApplyException("Can not OUTER_JOIN stream with " + joinStream.getClass().getSimpleName());
    }

    @Override
    public StreamWrapper apply(KTableWrapper input) {
        final StreamDataType resultValueType = StreamDataType.of(valueJoiner.resultType, input.valueType.notation, false);

        if (joinStream instanceof KTableWrapper) {
            return new KTableWrapper(
                    input.table.outerJoin(
                            ((KTableWrapper) joinStream).table,
                            new UserValueJoiner(valueJoiner),
                            Named.as(name),
                            Materialized.as(storeName)),
                    input.keyType,
                    resultValueType);
        }
        throw new KSMLApplyException("Can not OUTER_JOIN table with " + joinStream.getClass().getSimpleName());
    }
}
