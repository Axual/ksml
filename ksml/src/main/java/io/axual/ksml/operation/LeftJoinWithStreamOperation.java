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
import io.axual.ksml.definition.StreamDefinition;
import io.axual.ksml.generator.TopologyBuildContext;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;

import java.time.Duration;

public class LeftJoinWithStreamOperation extends DualStoreOperation {
    private static final String VALUEJOINER_NAME = "ValueJoiner";
    private final StreamDefinition joinStream;
    private final FunctionDefinition valueJoiner;
    private final JoinWindows joinWindows;

    public LeftJoinWithStreamOperation(DualStoreOperationConfig config, StreamDefinition joinStream, FunctionDefinition valueJoiner, Duration timeDifference, Duration gracePeriod) {
        super(config);
        this.joinStream = joinStream;
        this.valueJoiner = valueJoiner;
        this.joinWindows = joinWindowsOf(timeDifference, gracePeriod);
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input, TopologyBuildContext context) {
        /*    Kafka Streams method signature:
         *    <VO, VR> KStream<K, VR> leftJoin(
         *          final KStream<K, VO> otherStream,
         *          final ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joiner,
         *          final JoinWindows windows,
         *          final StreamJoined<K, V, VO> streamJoined)
         */

        checkNotNull(valueJoiner, VALUEJOINER_NAME.toLowerCase());
        final var k = input.keyType();
        final var v = input.valueType();
        final var otherStream = context.getStreamWrapper(joinStream);
        final var ko = otherStream.keyType();
        final var vo = otherStream.valueType();
        final var vr = streamDataTypeOf(firstSpecificType(valueJoiner, vo, v), false);
        checkType("Join stream keyType", ko, equalTo(k));
        final var joiner = userFunctionOf(context, VALUEJOINER_NAME, valueJoiner, vr, superOf(k), superOf(v), superOf(vo));
        final var thisStore = validateWindowStore(thisStore(), k, vr);
        final var otherStore = validateWindowStore(otherStore(), k, vr);
        final var streamJoined = streamJoinedOf(thisStore, otherStore, k, v, vo, joinWindows);
        final var userJoiner = valueJoinerWithKey(joiner, tags);
        final KStream<Object, Object> output = streamJoined != null
                ? input.stream.leftJoin(otherStream.stream, userJoiner, joinWindows, streamJoined)
                : input.stream.leftJoin(otherStream.stream, userJoiner, joinWindows);
        return new KStreamWrapper(output, k, vr);
    }
}
