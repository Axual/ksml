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


import io.axual.ksml.definition.StreamDefinition;
import io.axual.ksml.generator.TopologyBuildContext;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;

public class MergeOperation extends BaseOperation {
    private final StreamDefinition mergeStream;

    public MergeOperation(OperationConfig config, StreamDefinition mergeStream) {
        super(config);
        this.mergeStream = mergeStream;
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input, TopologyBuildContext context) {
        /*    Kafka Streams method signature:
         *    KStream<K, V> merge(
         *          final KStream<K, V> stream,
         *          final Named named)
         */

        final var otherStream = context.getStreamWrapper(mergeStream);
        final var k = input.keyType();
        final var v = input.valueType();
        checkType("Merge stream keyType", otherStream.keyType().userType(), equalTo(k));
        checkType("Merge stream valueType", otherStream.valueType().userType(), equalTo(v));
        final var named = namedOf();
        final var output = named != null
                ? input.stream.merge(otherStream.stream, named)
                : input.stream.merge(otherStream.stream);
        return new KStreamWrapper(output, k, v);
    }
}
