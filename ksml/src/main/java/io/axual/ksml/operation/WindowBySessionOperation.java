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


import io.axual.ksml.stream.KGroupedStreamWrapper;
import io.axual.ksml.stream.SessionWindowedKStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;
import org.apache.kafka.streams.kstream.SessionWindows;

public class WindowBySessionOperation extends BaseOperation {
    private final SessionWindows sessionWindows;

    public WindowBySessionOperation(OperationConfig config, SessionWindows sessionWindows) {
        super(config);
        this.sessionWindows = sessionWindows;
    }

    @Override
    public StreamWrapper apply(KGroupedStreamWrapper input) {
        final var k = input.keyType();
        final var v = input.valueType();
        return new SessionWindowedKStreamWrapper(input.groupedStream.windowedBy(sessionWindows), k, v);
    }
}
