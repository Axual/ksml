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

import io.axual.ksml.exception.KSMLTopologyException;
import io.axual.ksml.generator.StreamDataType;
import io.axual.ksml.stream.GlobalKTableWrapper;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.KTableWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.user.UserFunction;
import io.axual.ksml.user.UserKeyTransformer;
import io.axual.ksml.user.UserValueJoiner;

public class JoinOperation extends StoreOperation {
    private static final String KEYSELECTOR_NAME = "Mapper";
    private static final String VALUEJOINER_NAME = "ValueJoiner";
    private final StreamWrapper joinStream;
    private final UserFunction keyValueMapper;
    private final UserFunction valueJoiner;
    private final JoinWindows joinWindows;

    public JoinOperation(StoreOperationConfig config, KStreamWrapper joinStream, UserFunction valueJoiner, Duration joinWindowDuration) {
        super(config);
        this.joinStream = joinStream;
        this.keyValueMapper = null;
        this.valueJoiner = valueJoiner;
        this.joinWindows = JoinWindows.ofTimeDifferenceWithNoGrace(joinWindowDuration);
    }

    public JoinOperation(StoreOperationConfig config, KTableWrapper joinStream, UserFunction valueJoiner) {
        super(config);
        this.joinStream = joinStream;
        this.keyValueMapper = null;
        this.valueJoiner = valueJoiner;
        this.joinWindows = null;
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

        if (joinStream instanceof KStreamWrapper kStreamWrapper) {
            /*    Kafka Streams method signature:
             *    <VO, VR> KStream<K, VR> join(
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
                    input.stream.join(
                            kStreamWrapper.stream,
                            new UserValueJoiner(valueJoiner),
                            joinWindows,
                            StreamJoined.with(input.keyType().getSerde(), input.valueType().getSerde(), resultValueType.getSerde()).withName(name).withStoreName(store.name())),
                    input.keyType(),
                    resultValueType);
        }
        if (joinStream instanceof KTableWrapper kTableWrapper) {
            /*    Kafka Streams method signature:
             *    <VT, VR> KStream<K, VR> join(
             *          final KTable<K, VT> table,
             *          final ValueJoiner<? super V, ? super VT, ? extends VR> joiner,
             *          final Joined<K, V, VT> joined)
             */

            checkNotNull(valueJoiner, VALUEJOINER_NAME.toLowerCase());
            var k = input.keyType().userType().dataType();
            var v = input.valueType().userType().dataType();
            var vt = kTableWrapper.valueType().userType().dataType();
            var vr = valueJoiner.resultType.dataType();
            checkType("Join table keyType", kTableWrapper.keyType().userType().dataType(), equalTo(k));
            checkFunction(VALUEJOINER_NAME, valueJoiner, equalTo(vr), superOf(v), superOf(vt));

            return new KStreamWrapper(
                    input.stream.join(
                            kTableWrapper.table,
                            new UserValueJoiner(valueJoiner),
                            Joined.with(input.keyType().getSerde(), input.valueType().getSerde(), resultValueType.getSerde(), store.name())),
                    input.keyType(),
                    resultValueType);
        }
        if (joinStream instanceof GlobalKTableWrapper globalKTableWrapper) {
            /*    Kafka Streams method signature:
             *    <GK, GV, RV> KStream<K, RV> join(
             *          final GlobalKTable<GK, GV> globalTable,
             *          final KeyValueMapper<? super K, ? super V, ? extends GK> keySelector,
             *          final ValueJoiner<? super V, ? super GV, ? extends RV> joiner,
             *          final Named named)
             */

            checkNotNull(keyValueMapper, KEYSELECTOR_NAME.toLowerCase());
            checkNotNull(valueJoiner, VALUEJOINER_NAME.toLowerCase());
            var k = input.keyType().userType().dataType();
            var v = input.valueType().userType().dataType();
            var gk = globalKTableWrapper.keyType().userType().dataType();
            var gv = globalKTableWrapper.valueType().userType().dataType();
            var rv = valueJoiner.resultType.dataType();
            checkType("Join globalKTable keyType", globalKTableWrapper.keyType().userType().dataType(), equalTo(k));
            checkFunction(KEYSELECTOR_NAME, keyValueMapper, subOf(gk), superOf(k), superOf(v));
            checkFunction(VALUEJOINER_NAME, valueJoiner, subOf(rv), superOf(v), superOf(gv));

            return new KStreamWrapper(
                    input.stream.join(
                            globalKTableWrapper.globalTable,
                            new UserKeyTransformer(keyValueMapper),
                            new UserValueJoiner(valueJoiner),
                            Named.as(name)),
                    input.keyType(),
                    resultValueType);
        }
        throw new KSMLTopologyException("Can not JOIN stream with " + joinStream.getClass().getSimpleName());
    }

    @Override
    public StreamWrapper apply(KTableWrapper input) {
        final StreamDataType resultValueType = streamDataTypeOf(valueJoiner.resultType, false);

        if (joinStream instanceof KTableWrapper kTableWrapper) {
            /*    Kafka Streams method signature:
             *    <VO, VR> KTable<K, VR> join(
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
                    input.table.join(
                            kTableWrapper.table,
                            new UserValueJoiner(valueJoiner),
                            Named.as(name),
                            registerKeyValueStore(input.keyType(), resultValueType)),
                    input.keyType(),
                    resultValueType);
        }
        throw new KSMLTopologyException("Can not JOIN table with " + joinStream.getClass().getSimpleName());
    }
}
