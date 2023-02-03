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
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.StreamJoined;

import java.time.Duration;

import io.axual.ksml.exception.KSMLTopologyException;
import io.axual.ksml.generator.StreamDataType;
import io.axual.ksml.stream.BaseStreamWrapper;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.KTableWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.user.UserFunction;
import io.axual.ksml.user.UserValueJoiner;

public class OuterJoinOperation extends StoreOperation {
    private static final String VALUEJOINER_NAME = "ValueJoiner";
    private final BaseStreamWrapper joinStream;
    private final UserFunction valueJoiner;
    private final JoinWindows joinWindows;

    public OuterJoinOperation(StoreOperationConfig config, KStreamWrapper joinStream, UserFunction valueJoiner, Duration joinWindowDuration) {
        super(config);
        this.joinStream = joinStream;
        this.valueJoiner = valueJoiner;
        this.joinWindows = JoinWindows.ofTimeDifferenceWithNoGrace(joinWindowDuration);
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input) {
        final StreamDataType resultValueType = streamDataTypeOf(valueJoiner.resultType, false);

        if (joinStream instanceof KStreamWrapper kStreamWrapper) {
            /*    Kafka Streams method signature:
             *    <VO, VR> KStream<K, VR> outerJoin(
             *          final KStream<K, VO> otherStream,
             *          final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
             *          final JoinWindows windows,
             *          final StreamJoined<K, V, VO> streamJoined)
             */

            checkNotNull(valueJoiner, VALUEJOINER_NAME.toLowerCase());
            var k = input.keyType().userType().dataType();
            var v = input.valueType().userType().dataType();
            var vo = kStreamWrapper.valueType().userType().dataType();
            var vr = valueJoiner.resultType.dataType();
            checkType("Join stream keyType", kStreamWrapper.keyType().userType().dataType(), equalTo(k));
            checkFunction(VALUEJOINER_NAME, valueJoiner, equalTo(vr), superOf(v), superOf(vo));

            return new KStreamWrapper(
                    input.stream.outerJoin(
                            kStreamWrapper.stream,
                            new UserValueJoiner(valueJoiner),
                            joinWindows,
                            StreamJoined.with(input.keyType().getSerde(), input.valueType().getSerde(), resultValueType.getSerde()).withName(name).withStoreName(store.name())),
                    input.keyType(),
                    resultValueType);
        }
        throw new KSMLTopologyException("Can not OUTER_JOIN stream with " + joinStream.getClass().getSimpleName());
    }

    @Override
    public StreamWrapper apply(KTableWrapper input) {
        final StreamDataType resultValueType = streamDataTypeOf(valueJoiner.resultType, false);

        if (joinStream instanceof KTableWrapper kTableWrapper) {
            /*    Kafka Streams method signature:
             *    <VO, VR> KTable<K, VR> outerJoin(
             *          final KTable<K, VO> other,
             *          final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
             *          final Named named,
             *          final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized)
             */

            checkNotNull(valueJoiner, VALUEJOINER_NAME.toLowerCase());
            var k = input.keyType().userType().dataType();
            var v = input.valueType().userType().dataType();
            var vo = kTableWrapper.valueType().userType().dataType();
            var vr = valueJoiner.resultType.dataType();
            checkType("Join table keyType", kTableWrapper.keyType().userType().dataType(), equalTo(k));
            checkFunction(VALUEJOINER_NAME, valueJoiner, subOf(vr), superOf(v), superOf(vo));

            return new KTableWrapper(
                    input.table.outerJoin(
                            kTableWrapper.table,
                            new UserValueJoiner(valueJoiner),
                            Named.as(name),
                            registerKeyValueStore(input.keyType(), resultValueType)),
                    input.keyType(),
                    resultValueType);
        }
        throw new KSMLTopologyException("Can not OUTER_JOIN table with " + joinStream.getClass().getSimpleName());
    }
}
