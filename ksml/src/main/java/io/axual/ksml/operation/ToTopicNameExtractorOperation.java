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
import io.axual.ksml.data.type.StructType;
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.generator.TopologyBuildContext;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.type.UserType;
import io.axual.ksml.user.UserStreamPartitioner;
import io.axual.ksml.user.UserTopicNameExtractor;

import static io.axual.ksml.dsl.RecordContextSchema.RECORD_CONTEXT_SCHEMA;

public class ToTopicNameExtractorOperation extends BaseOperation {
    private static final String PARTITIONER_NAME = "Partitioner";
    private static final String TOPICNAMEEXTRACTOR_NAME = "TopicNameExtractor";
    private final FunctionDefinition topicNameExtractor;
    private final FunctionDefinition partitioner;

    public ToTopicNameExtractorOperation(OperationConfig config, FunctionDefinition topicNameExtractor, FunctionDefinition partitioner) {
        super(config);
        this.topicNameExtractor = topicNameExtractor;
        this.partitioner = partitioner;
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input, TopologyBuildContext context) {
        /*    Kafka Streams method signature:
         *    void to(
         *          final TopicNameExtractor<K, V> topicExtractor,
         *          final Produced<K, V> produced)
         */

        final var k = input.keyType();
        final var v = input.valueType();
        final var topicNameType = new UserType(DataString.DATATYPE);
        final var recordContextType = new UserType(new StructType(RECORD_CONTEXT_SCHEMA));
        final var extract = userFunctionOf(context, TOPICNAMEEXTRACTOR_NAME, topicNameExtractor, topicNameType, superOf(k), superOf(v), superOf(recordContextType));
        final var userExtract = new UserTopicNameExtractor(extract, tags);
        final var part = userFunctionOf(context, PARTITIONER_NAME, partitioner, equalTo(DataInteger.DATATYPE), equalTo(DataString.DATATYPE), superOf(k), superOf(v), equalTo(DataInteger.DATATYPE));
        final var userPart = part != null ? new UserStreamPartitioner(part, tags) : null;
        final var produced = producedOf(k, v, userPart);
        if (produced != null)
            input.stream.to(userExtract, produced);
        else
            input.stream.to(userExtract);
        return null;
    }
}
