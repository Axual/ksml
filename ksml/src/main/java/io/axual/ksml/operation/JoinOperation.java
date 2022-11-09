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
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.StreamJoined;

import java.time.Duration;

import io.axual.ksml.exception.KSMLApplyException;
import io.axual.ksml.generator.StreamDataType;
import io.axual.ksml.stream.GlobalKTableWrapper;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.KTableWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.user.UserFunction;
import io.axual.ksml.user.UserKeyTransformer;
import io.axual.ksml.user.UserValueJoiner;

public class JoinOperation extends StoreOperation {
    private final StreamWrapper joinStream;
    private final UserFunction keyValueMapper;
    private final UserFunction valueJoiner;
    private final JoinWindows joinWindows;

    public JoinOperation(StoreOperationConfig config, KStreamWrapper joinStream, UserFunction valueJoiner, Duration joinWindowDuration) {
        super(config);
        this.joinStream = joinStream;
        this.keyValueMapper = null;
        this.valueJoiner = valueJoiner;
        this.joinWindows = JoinWindows.of(joinWindowDuration);
    }

    public JoinOperation(StoreOperationConfig config, KTableWrapper joinStream, UserFunction valueJoiner, Duration joinWindowDuration) {
        super(config);
        this.joinStream = joinStream;
        this.keyValueMapper = null;
        this.valueJoiner = valueJoiner;
        this.joinWindows = JoinWindows.of(joinWindowDuration);
    }

    public JoinOperation(StoreOperationConfig config, GlobalKTableWrapper joinStream, UserFunction keyValueMapper, UserFunction valueJoiner) {
        super(config);
        this.joinStream = joinStream;
        this.keyValueMapper = keyValueMapper;
        this.valueJoiner = valueJoiner;
        this.joinWindows = null;
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input) {
        final StreamDataType resultValueType = streamDataTypeOf(valueJoiner.resultType, false);

        if (joinStream instanceof KStreamWrapper wrapper) {
            return new KStreamWrapper(
                    input.stream.join(
                            wrapper.stream,
                            new UserValueJoiner(valueJoiner),
                            joinWindows,
                            StreamJoined.with(input.keyType().getSerde(), input.valueType().getSerde(), resultValueType.getSerde()).withName(name).withStoreName(store.name)),
                    input.keyType(),
                    resultValueType);
        }
        if (joinStream instanceof KTableWrapper wrapper) {
            return new KStreamWrapper(
                    input.stream.join(
                            wrapper.table,
                            new UserValueJoiner(valueJoiner),
                            Joined.with(input.keyType().getSerde(), input.valueType().getSerde(), resultValueType.getSerde(), store.name)),
                    input.keyType(),
                    resultValueType);
        }
        if (joinStream instanceof GlobalKTableWrapper wrapper) {
            return new KStreamWrapper(
                    input.stream.join(
                            wrapper.globalTable,
                            new UserKeyTransformer(keyValueMapper),
                            new UserValueJoiner(valueJoiner),
                            Named.as(name)),
                    input.keyType(),
                    resultValueType);
        }
        throw new KSMLApplyException("Can not JOIN stream with " + joinStream.getClass().getSimpleName());
    }

    @Override
    public StreamWrapper apply(KTableWrapper input) {
        final StreamDataType resultValueType = streamDataTypeOf(valueJoiner.resultType, false);

        if (joinStream instanceof KTableWrapper wrapper) {
            return new KTableWrapper(
                    input.table.join(
                            wrapper.table,
                            new UserValueJoiner(valueJoiner),
                            Named.as(name),
                            registerKeyValueStore(input.keyType(), resultValueType)),
                    input.keyType(),
                    resultValueType);
        }
        throw new KSMLApplyException("Can not JOIN table with " + joinStream.getClass().getSimpleName());
    }
}
