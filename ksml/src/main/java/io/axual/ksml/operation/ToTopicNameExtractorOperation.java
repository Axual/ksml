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



import org.apache.kafka.streams.kstream.Produced;

import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.user.UserFunction;
import io.axual.ksml.user.UserTopicNameExtractor;

public class ToTopicNameExtractorOperation extends BaseOperation {
    private final UserFunction topicNameExtractor;

    public ToTopicNameExtractorOperation(UserFunction topicNameExtractor) {
        this.topicNameExtractor = topicNameExtractor;
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input) {
        input.stream.to(new UserTopicNameExtractor(topicNameExtractor), Produced.with(input.keyType.serde, input.valueType.serde));
        return null;
    }
}
