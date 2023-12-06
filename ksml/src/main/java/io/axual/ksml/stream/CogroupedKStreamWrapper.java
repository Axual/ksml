package io.axual.ksml.stream;

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


import io.axual.ksml.generator.StreamDataType;
import io.axual.ksml.generator.TopologyBuildContext;
import io.axual.ksml.operation.StreamOperation;
import org.apache.kafka.streams.kstream.CogroupedKStream;

public class CogroupedKStreamWrapper extends BaseStreamWrapper {
    public final CogroupedKStream<Object, Object> cogroupedStream;

    public CogroupedKStreamWrapper(CogroupedKStream<Object, Object> cogroupedStream, StreamDataType key, StreamDataType value) {
        super(key, value);
        this.cogroupedStream = cogroupedStream;
    }

    @Override
    public StreamWrapper apply(StreamOperation operation, TopologyBuildContext context) {
        return operation.apply(this, context);
    }
}
