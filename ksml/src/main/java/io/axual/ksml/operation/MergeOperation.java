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


import org.apache.kafka.streams.kstream.Named;

import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;

public class MergeOperation extends BaseOperation {
    private final KStreamWrapper mergeStream;

    public MergeOperation(OperationConfig config, KStreamWrapper mergeStream) {
        super(config);
        this.mergeStream = mergeStream;
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input) {
        /*
         *    Kafka Streams method signature:
         *    KStream<K, V> merge(
         *          final KStream<K, V> stream,
         *          final Named named);
         */

        final var k = streamDataTypeOf(input.keyType().userType(), true);
        final var v = streamDataTypeOf(input.valueType().userType(), false);
        checkType("Merge stream keyType", mergeStream.keyType().userType(), equalTo(k));
        checkType("Merge stream valueType", mergeStream.valueType().userType(), equalTo(v));
        final var output = name != null
                ? input.stream.merge(mergeStream.stream, Named.as(name))
                : input.stream.merge(mergeStream.stream);
        return new KStreamWrapper(output, k, v);
    }
}
