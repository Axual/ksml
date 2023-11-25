package io.axual.ksml.runner.backend;

/*-
 * ========================LICENSE_START=================================
 * KSML Runner
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


import org.apache.kafka.streams.KafkaStreams;

import io.axual.ksml.rest.server.StreamsQuerier;

public interface Backend extends AutoCloseable, Runnable {
    enum State {
        STARTING,
        STARTED,
        STOPPING,
        STOPPED,
        FAILED
    }

    State getState();

    StreamsQuerier getQuerier();

    default State convertStreamsState(KafkaStreams.State state) {
        return switch (state) {
            case CREATED, REBALANCING -> State.STARTING;
            case RUNNING -> State.STARTED;
            case PENDING_SHUTDOWN -> State.STOPPING;
            case NOT_RUNNING -> State.STOPPED;
            case PENDING_ERROR, ERROR -> State.FAILED;
        };
    }
}
