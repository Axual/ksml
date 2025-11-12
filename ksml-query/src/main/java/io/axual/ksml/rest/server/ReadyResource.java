package io.axual.ksml.rest.server;

/*-
 * ========================LICENSE_START=================================
 * KSML Queryable State Store
 * %%
 * Copyright (C) 2021 - 2024 Axual B.V.
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

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Response;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;

import static io.axual.ksml.rest.server.ComponentState.NOT_APPLICABLE;
import static io.axual.ksml.rest.server.ComponentState.STARTED;
import static io.axual.ksml.rest.server.ComponentState.STOPPED;
import static io.axual.ksml.rest.server.ComponentState.STOPPING;

@Slf4j(topic = "ksml.rest.service.ready")
@Path("ready")
public class ReadyResource {
    private static final Set<ComponentState> PRODUCER_READY_STATES = Set.of(NOT_APPLICABLE, STARTED, STOPPING, STOPPED);
    private static final Set<ComponentState> STREAMS_READY_STATES = Set.of(NOT_APPLICABLE, STARTED);

    @GET()
    public Response getReadyState() {
        final var querier = GlobalState.INSTANCE.querier();

        if (querier == null) {
            // Service has not started yet
            log.trace("KSML Not Ready -No querier available, still in startup");
            return Response.serverError().build();
        }

        final var producerState = querier.getProducerState();
        final var streamRunnerState = querier.getStreamRunnerState();

        if (producerState == NOT_APPLICABLE && streamRunnerState == NOT_APPLICABLE) {
            log.trace("KSML Not Ready - Both producerState and streamRunnerState are disabled");
            return Response.serverError().build();
        }

        if (PRODUCER_READY_STATES.contains(producerState) && STREAMS_READY_STATES.contains(streamRunnerState)) {
            // KSML is running, return HTTP Status code 204 (OK, No Content) if components
            log.trace("KSML Ready - producer state '{}' stream runner state '{}' ", producerState, streamRunnerState);
            return Response.noContent().build();
        } else {
            log.trace("KSML Not Ready - producer state '{}' stream runner state '{}' ", producerState, streamRunnerState);
            return Response.serverError().build();
        }
    }
}
