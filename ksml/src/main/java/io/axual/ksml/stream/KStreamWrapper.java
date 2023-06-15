package io.axual.ksml.stream;

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


import io.axual.ksml.generator.StreamDataType;
import io.axual.ksml.operation.StreamOperation;
import org.apache.kafka.streams.kstream.KStream;

public class KStreamWrapper extends BaseStreamWrapper {
    public final KStream<Object, Object> stream;

    public KStreamWrapper(KStream<Object, Object> stream, StreamDataType keyType, StreamDataType valueType) {
        super(keyType, valueType);
        this.stream = stream;
    }

    @Override
    public StreamWrapper apply(StreamOperation operation) {
        return operation.apply(this);
    }
}
