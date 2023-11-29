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


import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.type.StructType;
import io.axual.ksml.data.type.UserType;
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.generator.TopologyBuildContext;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.user.UserTopicNameExtractor;
import org.apache.kafka.streams.kstream.Produced;

import static io.axual.ksml.dsl.RecordContextSchema.RECORD_CONTEXT_SCHEMA;

public class ToTopicNameExtractorOperation extends BaseOperation {
    private static final String TOPICNAMEEXTRACTOR_NAME = "TopicNameExtractor";
    private final FunctionDefinition topicNameExtractor;

    public ToTopicNameExtractorOperation(OperationConfig config, FunctionDefinition topicNameExtractor) {
        super(config);
        this.topicNameExtractor = topicNameExtractor;
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input, TopologyBuildContext context) {
        final var k = input.keyType();
        final var v = input.valueType();
        final var topicNameType = new UserType(DataString.DATATYPE);
        final var recordContextType = new UserType(new StructType(RECORD_CONTEXT_SCHEMA));
        final var extractor = checkFunction(TOPICNAMEEXTRACTOR_NAME, topicNameExtractor, topicNameType, superOf(k), superOf(v), superOf(recordContextType));
        final var userExtractor = new UserTopicNameExtractor(context.createUserFunction(extractor));
        var produced = Produced.with(input.keyType().getSerde(), input.valueType().getSerde());
        if (name != null) produced = produced.withName(name);
        input.stream.to(userExtractor, produced);
        return null;
    }
}
