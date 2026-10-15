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


import io.axual.ksml.exception.TopologyException;
import io.axual.ksml.generator.StreamDataType;
import io.axual.ksml.generator.TopologyBuildContext;
import io.axual.ksml.operation.StreamOperation;
import org.apache.kafka.streams.kstream.CogroupedKStream;
import org.apache.kafka.streams.kstream.KGroupedStream;

import java.util.List;

public class CogroupedKStreamWrapper extends BaseStreamWrapper {
    public final CogroupedKStream<Object, Object> cogroupedStream;
    // The grouped streams in this cogroup. Kafka Streams keeps one aggregator per grouped stream, so adding the same
    // grouped stream again would silently replace its aggregator.
    public final List<KGroupedStream<Object, Object>> groupedStreams;
    // Kafka Streams changes a cogrouped stream in place. After a stream is added, other pipelines that continue from
    // this cogrouped stream (saved with "as") would also get the added stream
    private boolean extended = false;

    public CogroupedKStreamWrapper(CogroupedKStream<Object, Object> cogroupedStream, List<KGroupedStream<Object, Object>> groupedStreams, StreamDataType key, StreamDataType value) {
        super("CogroupedStream", key, value);
        this.cogroupedStream = cogroupedStream;
        this.groupedStreams = List.copyOf(groupedStreams);
    }

    public boolean contains(KGroupedStream<Object, Object> groupedStream) {
        return groupedStreams.contains(groupedStream);
    }

    public void checkNotExtended() {
        if (extended) {
            throw new TopologyException(this + " (or a window of it) already has a stream added by another pipeline. Add all streams to a cogroup in one pipeline");
        }
    }

    public void markExtended() {
        checkNotExtended();
        extended = true;
    }

    @Override
    public StreamWrapper apply(StreamOperation operation, TopologyBuildContext context) {
        return operation.apply(this, context);
    }
}
