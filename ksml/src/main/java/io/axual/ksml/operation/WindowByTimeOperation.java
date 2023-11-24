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


import io.axual.ksml.exception.KSMLTopologyException;
import io.axual.ksml.stream.KGroupedStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.stream.TimeWindowedKStreamWrapper;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.TimeWindows;

public class WindowByTimeOperation extends BaseOperation {
    private final SlidingWindows slidingWindows;
    private final TimeWindows timeWindows;

    public WindowByTimeOperation(OperationConfig config, SlidingWindows slidingWindows) {
        super(config);
        this.slidingWindows = slidingWindows;
        this.timeWindows = null;
    }

    public WindowByTimeOperation(OperationConfig config, TimeWindows timeWindows) {
        super(config);
        this.slidingWindows = null;
        this.timeWindows = timeWindows;
    }

    @Override
    public StreamWrapper apply(KGroupedStreamWrapper input) {
        final var k = input.keyType();
        final var v = input.valueType();
        if (slidingWindows != null) {
            return new TimeWindowedKStreamWrapper(input.groupedStream.windowedBy(slidingWindows), k, v);
        }
        if (timeWindows != null) {
            return new TimeWindowedKStreamWrapper(input.groupedStream.windowedBy(timeWindows), k, v);
        }
        throw new KSMLTopologyException("Operation " + name + ". Error applying WINDOW BY to " + input);
    }
}
