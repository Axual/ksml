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


import io.axual.ksml.definition.Ref;
import io.axual.ksml.definition.StreamDefinition;
import io.axual.ksml.generator.TopologyBuildContext;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;
import org.apache.kafka.streams.kstream.Named;

public class MergeOperation extends BaseOperation {
    private final Ref<StreamDefinition> mergeStreamDefinition;

    public MergeOperation(OperationConfig config, Ref<StreamDefinition> mergeStreamDefinition) {
        super(config);
        this.mergeStreamDefinition = mergeStreamDefinition;
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input, TopologyBuildContext context) {
        /*    Kafka Streams method signature:
         *    KStream<K, V> merge(
         *          final KStream<K, V> stream,
         *          final Named named)
         */

        final var k = input.keyType();
        final var v = input.valueType();
        final var mergeStream = context.getStreamWrapper(mergeStreamDefinition, "merge");
        checkType("Merge stream keyType", mergeStream.keyType().userType(), equalTo(k));
        checkType("Merge stream valueType", mergeStream.valueType().userType(), equalTo(v));
        final var output = name != null
                ? input.stream.merge(mergeStream.stream, Named.as(name))
                : input.stream.merge(mergeStream.stream);
        return new KStreamWrapper(output, k, v);
    }
}
