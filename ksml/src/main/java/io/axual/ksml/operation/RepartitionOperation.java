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

import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.generator.TopologyBuildContext;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.user.UserStreamPartitioner;
import org.apache.kafka.streams.kstream.KStream;

public class RepartitionOperation extends BaseOperation {
    private static final String PARTITIONER_NAME = "Partitioner";
    private final FunctionDefinition partitioner;

    public RepartitionOperation(OperationConfig config, FunctionDefinition partitioner) {
        super(config);
        this.partitioner = partitioner;
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input, TopologyBuildContext context) {
        /*    Kafka Streams method signature:
         *    KStream<K, V> repartition(
         *          final Repartitioned<K, V> repartitioned)
         */

        checkNotNull(partitioner, "Partitioner must be defined");
        final var k = input.keyType();
        final var v = input.valueType();
        final var part = userFunctionOf(context, PARTITIONER_NAME, partitioner, equalTo(DataInteger.DATATYPE), equalTo(DataString.DATATYPE), superOf(k), superOf(v), equalTo(DataInteger.DATATYPE));
        final var userPart = part != null ? new UserStreamPartitioner(part) : null;
        final var repartitioned = repartitionedOf(k, v, userPart);
        final KStream<Object, Object> output = repartitioned != null
                ? input.stream.repartition(repartitioned)
                : input.stream.repartition();
        return new KStreamWrapper(output, k, v);
    }
}
