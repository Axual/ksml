package io.axual.ksml.operation;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2023 Axual B.V.
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


import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.definition.Ref;
import io.axual.ksml.definition.TopicDefinition;
import io.axual.ksml.definition.TypedRef;
import io.axual.ksml.exception.KSMLTopologyException;
import io.axual.ksml.generator.TopologyBuildContext;
import io.axual.ksml.stream.GlobalKTableWrapper;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.KTableWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.user.UserFunction;
import io.axual.ksml.user.UserKeyTransformer;
import io.axual.ksml.user.UserValueJoiner;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.StreamJoined;

import java.time.Duration;
import java.util.HashMap;

public class JoinOperation extends StoreOperation {
    private static final String KEYSELECTOR_NAME = "Mapper";
    private static final String VALUEJOINER_NAME = "ValueJoiner";
    private final TypedRef joinStream;
    private final Ref<FunctionDefinition> keyValueMapper;
    private final Ref<FunctionDefinition> valueJoiner;
    private final JoinWindows joinWindows;

    public JoinOperation(StoreOperationConfig config, TypedRef joinStream, Ref<FunctionDefinition> valueJoiner, Duration joinWindowDuration) {
        super(config);
        this.joinStream = joinStream;
        this.keyValueMapper = null;
        this.valueJoiner = valueJoiner;
        this.joinWindows = JoinWindows.ofTimeDifferenceWithNoGrace(joinWindowDuration);
    }

    public JoinOperation(StoreOperationConfig config, TypedRef joinStream, Ref<FunctionDefinition> valueJoiner) {
        super(config);
        this.joinStream = joinStream;
        this.keyValueMapper = null;
        this.valueJoiner = valueJoiner;
        this.joinWindows = null;
    }

    public JoinOperation(StoreOperationConfig config, TypedRef joinStream, Ref<FunctionDefinition> keyValueMapper, Ref<FunctionDefinition> valueJoiner) {
        super(config);
        this.joinStream = joinStream;
        this.keyValueMapper = keyValueMapper;
        this.valueJoiner = valueJoiner;
        this.joinWindows = null;
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input, TopologyBuildContext context) {
        checkNotNull(valueJoiner, VALUEJOINER_NAME.toLowerCase());
        final var k = input.keyType();
        final var v = input.valueType();

        if (joinStream.type() == TypedRef.TopicType.STREAM) {
            /*    Kafka Streams method signature:
             *    <VO, VR> KStream<K, VR> join(
             *          final KStream<K, VO> otherStream,
             *          final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
             *          final JoinWindows windows,
             *          final StreamJoined<K, V, VO> streamJoined)
             */

            final var otherStream = context.getStreamWrapper(joinStream.ref(), "join stream");
            final var vo = otherStream.valueType();
            final var vr = streamDataTypeOf(firstSpecificType(valueJoiner, vo, v), false);
            checkType("Join stream keyType", vo, equalTo(k));
            checkFunction(VALUEJOINER_NAME, valueJoiner, vr, superOf(v), superOf(vo));
            final var windowStore = validateWindowStore(context.lookupStore(store), k, vr);

            var joined = StreamJoined.with(k.getSerde(), v.getSerde(), vo.getSerde());
            if (name != null) joined = joined.withName(name);
            if (windowStore != null) {
                if (windowStore.name() != null) joined = joined.withStoreName(windowStore.name());
                joined = windowStore.logging() ? joined.withLoggingEnabled(new HashMap<>()) : joined.withLoggingDisabled();
            }

            final var joiner = new UserValueJoiner(valueJoiner);
            final var output = (KStream) input.stream.join(otherStream.stream, joiner, joinWindows, joined);
            return new KStreamWrapper(output, k, vr);
        }

        if (joinStream instanceof KTableWrapper otherTable) {
            /*    Kafka Streams method signature:
             *    <VT, VR> KStream<K, VR> join(
             *          final KTable<K, VT> table,
             *          final ValueJoiner<? super V, ? super VT, ? extends VR> joiner,
             *          final Joined<K, V, VT> joined)
             */

            final var vt = otherTable.valueType();
            final var vr = streamDataTypeOf(firstSpecificType(valueJoiner, vt, v), false);
            checkType("Join table keyType", otherTable.keyType(), equalTo(k));
            checkFunction(VALUEJOINER_NAME, valueJoiner, vr, superOf(v), superOf(vt));
            var joined = Joined.with(k.getSerde(), v.getSerde(), vt.getSerde());
            if (name != null) joined = joined.withName(name);
            final var output = (KStream) input.stream.join(otherTable.table, new UserValueJoiner(valueJoiner), joined);
            return new KStreamWrapper(output, k, vr);
        }

        if (joinStream instanceof GlobalKTableWrapper otherGlobalKTable) {
            /*    Kafka Streams method signature:
             *    <GK, GV, RV> KStream<K, RV> join(
             *          final GlobalKTable<GK, GV> globalTable,
             *          final KeyValueMapper<? super K, ? super V, ? extends GK> keySelector,
             *          final ValueJoiner<? super V, ? super GV, ? extends RV> joiner,
             *          final Named named)
             */

            checkNotNull(keyValueMapper, KEYSELECTOR_NAME.toLowerCase());
            final var gk = otherGlobalKTable.keyType();
            final var gv = otherGlobalKTable.valueType();
            final var rv = streamDataTypeOf(firstSpecificType(valueJoiner, gv, v), false);
            checkType("Join globalKTable keyType", otherGlobalKTable.keyType(), equalTo(k));
            checkFunction(KEYSELECTOR_NAME, keyValueMapper, subOf(gk), gk, superOf(k), superOf(v));
            checkFunction(VALUEJOINER_NAME, valueJoiner, rv, superOf(v), superOf(gv));
            final var output = name != null
                    ? (KStream) input.stream.join(
                    otherGlobalKTable.globalTable,
                    new UserKeyTransformer(keyValueMapper),
                    new UserValueJoiner(valueJoiner),
                    Named.as(name))
                    : (KStream) input.stream.join(
                    otherGlobalKTable.globalTable,
                    new UserKeyTransformer(keyValueMapper),
                    new UserValueJoiner(valueJoiner));
            return new KStreamWrapper(output, k, rv);
        }

        throw new KSMLTopologyException("Can not JOIN stream with " + joinStream.getClass().getSimpleName());
    }

    @Override
    public StreamWrapper apply(KTableWrapper input, TopologyBuildContext context) {
        checkNotNull(valueJoiner, VALUEJOINER_NAME.toLowerCase());
        final var k = input.keyType();
        final var v = input.valueType();

        if (joinStream instanceof KTableWrapper kTableWrapper) {
            /*    Kafka Streams method signature:
             *    <VO, VR> KTable<K, VR> join(
             *          final KTable<K, VO> other,
             *          final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
             *          final Named named,
             *          final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized)
             */

            final var vo = kTableWrapper.valueType();
            final var vr = streamDataTypeOf(firstSpecificType(valueJoiner, vo, v), false);
            checkType("Join table keyType", kTableWrapper.keyType(), equalTo(k));
            checkFunction(VALUEJOINER_NAME, valueJoiner, subOf(vr), vr, superOf(v), superOf(vo));
            final var kvStore = validateKeyValueStore(context.lookupStore(store), k, vr);
            if (kvStore != null) {
                final var mat = materialize(kvStore);
                final var output = name != null
                        ? input.table.join(kTableWrapper.table, new UserValueJoiner(valueJoiner), Named.as(name), mat)
                        : input.table.join(kTableWrapper.table, new UserValueJoiner(valueJoiner), mat);
                return new KTableWrapper(output, k, vr);
            }
        }
        throw new KSMLTopologyException("Can not JOIN table with " + joinStream.getClass().getSimpleName());
    }
}
