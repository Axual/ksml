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


import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.TimeWindows;

import io.axual.ksml.exception.KSMLTopologyException;
import io.axual.ksml.stream.KGroupedStreamWrapper;
import io.axual.ksml.stream.SessionWindowedKStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.stream.TimeWindowedKStreamWrapper;

public class WindowedByOperation extends BaseOperation {
    private final SessionWindows sessionWindows;
    private final SlidingWindows slidingWindows;
    private final TimeWindows timeWindows;

    public WindowedByOperation(OperationConfig config, SessionWindows sessionWindows) {
        super(config);
        this.sessionWindows = sessionWindows;
        this.slidingWindows = null;
        this.timeWindows = null;
    }

    public WindowedByOperation(OperationConfig config, SlidingWindows slidingWindows) {
        super(config);
        this.sessionWindows = null;
        this.slidingWindows = slidingWindows;
        this.timeWindows = null;
    }

    public WindowedByOperation(OperationConfig config, TimeWindows timeWindows) {
        super(config);
        this.sessionWindows = null;
        this.slidingWindows = null;
        this.timeWindows = timeWindows;
    }

    @Override
    public StreamWrapper apply(KGroupedStreamWrapper input) {
        if (sessionWindows != null) {
            return new SessionWindowedKStreamWrapper(input.groupedStream.windowedBy(sessionWindows), input.keyType(), input.valueType());
        }
        if (slidingWindows != null) {
            return new TimeWindowedKStreamWrapper(input.groupedStream.windowedBy(slidingWindows), input.keyType(), input.valueType());
        }
        if (timeWindows != null) {
            return new TimeWindowedKStreamWrapper(input.groupedStream.windowedBy(timeWindows), input.keyType(), input.valueType());
        }
        throw new KSMLTopologyException("Operation " + name + ". Error applying WINDOW BY to " + input);
    }
}
