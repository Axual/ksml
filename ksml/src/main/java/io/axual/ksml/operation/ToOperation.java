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


import io.axual.ksml.data.exception.ExecutionException;
import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.definition.TopicDefinition;
import io.axual.ksml.generator.TopologyBuildContext;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.user.UserStreamPartitioner;

public class ToOperation extends BaseOperation {
    private static final String PARTITIONER_NAME = "Partitioner";
    public final TopicDefinition topic;
    private final FunctionDefinition partitioner;

    public ToOperation(OperationConfig config, TopicDefinition topic, FunctionDefinition partitioner) {
        super(config);
        this.topic = topic;
        this.partitioner = partitioner;
        if (topic.topic() == null) {
            throw new ExecutionException("Can not produce to NULL target");
        }
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input, TopologyBuildContext context) {
        /*    Kafka Streams method signature:
         *    void to(
         *          final String topic,
         *          final Produced<K, V> produced)
         */

        final var k = input.keyType().flatten();
        final var v = input.valueType().flatten();
        final var kt = streamDataTypeOf(firstSpecificType(topic.keyType(), k.userType()), true).flatten();
        final var vt = streamDataTypeOf(firstSpecificType(topic.valueType(), v.userType()), false).flatten();
        // Perform a dataType check to see if the key/value data types received matches the stream definition's types
        checkType("Target topic keyType", kt, superOf(k));
        checkType("Target topic valueType", vt, superOf(v));
        final var part = userFunctionOf(context, PARTITIONER_NAME, partitioner, equalTo(DataInteger.DATATYPE), equalTo(DataString.DATATYPE), superOf(k), superOf(v), equalTo(DataInteger.DATATYPE));
        final var userPart = part != null ? new UserStreamPartitioner(part, tags) : null;
        final var produced = producedOf(kt, vt, userPart);
        if (produced != null)
            input.stream.to(topic.topic(), produced);
        else
            input.stream.to(topic.topic());
        return null;
    }
}
