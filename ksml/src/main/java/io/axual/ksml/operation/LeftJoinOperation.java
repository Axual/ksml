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


import io.axual.ksml.exception.KSMLTopologyException;
import io.axual.ksml.stream.BaseStreamWrapper;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.KTableWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.user.UserFunction;
import io.axual.ksml.user.UserValueJoiner;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.StreamJoined;

import java.time.Duration;
import java.util.HashMap;

public class LeftJoinOperation extends StoreOperation {
    private static final String VALUEJOINER_NAME = "ValueJoiner";
    private final BaseStreamWrapper joinStream;
    private final UserFunction valueJoiner;
    private final Duration joinWindowsDuration;

    public LeftJoinOperation(StoreOperationConfig config, KStreamWrapper joinStream, UserFunction valueJoiner, Duration joinWindowDuration) {
        super(config);
        this.joinStream = joinStream;
        this.valueJoiner = valueJoiner;
        this.joinWindowsDuration = joinWindowDuration;
    }

    public LeftJoinOperation(StoreOperationConfig config, KTableWrapper joinStream, UserFunction valueJoiner, Duration joinWindowDuration) {
        super(config);
        this.joinStream = joinStream;
        this.valueJoiner = valueJoiner;
        this.joinWindowsDuration = joinWindowDuration;
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input) {
        checkNotNull(valueJoiner, VALUEJOINER_NAME.toLowerCase());
        final var k = input.keyType();
        final var v = input.valueType();

        if (joinStream instanceof KStreamWrapper kStreamWrapper) {
            /*    Kafka Streams method signature:
             *    <VO, VR> KStream<K, VR> leftJoin(
             *          final KStream<K, VO> otherStream,
             *          final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
             *          final JoinWindows windows,
             *          final StreamJoined<K, V, VO> streamJoined)
             */

            final var vo = kStreamWrapper.valueType();
            final var vr = streamDataTypeOf(firstSpecificType(valueJoiner, vo, v), false);
            checkType("Join stream keyType", kStreamWrapper.keyType(), equalTo(k));
            checkFunction(VALUEJOINER_NAME, valueJoiner, vr, superOf(v), superOf(vo));
            var joined = StreamJoined.with(k.getSerde(), v.getSerde(), vr.getSerde());
            if (name != null) joined = joined.withName(name);
            if (store != null) {
                if (store.name() != null) joined = joined.withStoreName(store.name());
                joined = store.logging() ? joined.withLoggingEnabled(new HashMap<>()) : joined.withLoggingDisabled();
            }

            final var output = (KStream) input.stream.leftJoin(
                    kStreamWrapper.stream,
                    new UserValueJoiner(valueJoiner),
                    JoinWindows.ofTimeDifferenceWithNoGrace(joinWindowsDuration),
                    joined);
            return new KStreamWrapper(output, k, vr);
        }

        if (joinStream instanceof KTableWrapper kTableWrapper) {
            /*    Kafka Streams method signature:
             *    <VT, VR> KStream<K, VR> leftJoin(
             *          final KTable<K, VT> table,
             *          final ValueJoiner<? super V, ? super VT, ? extends VR> joiner,
             *          final Joined<K, V, VT> joined)
             */

            final var vt = kTableWrapper.valueType();
            final var vr = streamDataTypeOf(firstSpecificType(valueJoiner, vt, v), false);
            checkType("Join table keyType", kTableWrapper.keyType(), equalTo(k));
            checkFunction(VALUEJOINER_NAME, valueJoiner, vr, superOf(v), superOf(vt));
            var joined = Joined.with(k.getSerde(), v.getSerde(), vr.getSerde());
            if (name != null) joined = joined.withName(name);
            final var output = (KStream) input.stream.leftJoin(
                    kTableWrapper.table,
                    new UserValueJoiner(valueJoiner),
                    joined);
            return new KStreamWrapper(output, k, vr);
        }
        throw new KSMLTopologyException("Can not LEFT_JOIN stream with " + joinStream.getClass().getSimpleName());
    }

    @Override
    public StreamWrapper apply(KTableWrapper input) {
        checkNotNull(valueJoiner, VALUEJOINER_NAME.toLowerCase());
        final var k = input.keyType();
        final var v = input.valueType();

        if (joinStream instanceof KTableWrapper kTableWrapper) {
            /*    Kafka Streams method signature:
             *    <VO, VR> KTable<K, VR> leftJoin(
             *          final KTable<K, VO> other,
             *          final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
             *          final Named named,
             *          final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized)
             */

            final var vo = kTableWrapper.valueType();
            final var vr = streamDataTypeOf(firstSpecificType(valueJoiner, vo, v), false);
            checkType("Join table keyType", kTableWrapper.keyType(), equalTo(k));
            checkFunction(VALUEJOINER_NAME, valueJoiner, subOf(vr), vr, superOf(v), superOf(vo));
            final var kvStore = validateKeyValueStore(store, k, vr);
            if (kvStore != null) {
                final var mat = materialize(kvStore);
                final var output = name != null
                        ? input.table.leftJoin(kTableWrapper.table, new UserValueJoiner(valueJoiner), Named.as(name), mat)
                        : input.table.leftJoin(kTableWrapper.table, new UserValueJoiner(valueJoiner), mat);
                return new KTableWrapper(output, k, vr);
            }

            final var output = name != null
                    ? (KTable) input.table.leftJoin(kTableWrapper.table, new UserValueJoiner(valueJoiner), Named.as(name))
                    : (KTable) input.table.leftJoin(kTableWrapper.table, new UserValueJoiner(valueJoiner));
            return new KTableWrapper(output, k, vr);
        }

        throw new KSMLTopologyException("Can not LEFT_JOIN table with " + joinStream.getClass().getSimpleName());
    }
}
