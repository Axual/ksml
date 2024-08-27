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

import java.util.Set;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Response;
import lombok.extern.slf4j.Slf4j;

import static io.axual.ksml.rest.server.ComponentState.*;

@Slf4j
@Path("ready")
public class ReadyResource {
    private static final Set<ComponentState> NOT_RUNNING_STATES = Set.of(NOT_APPLICABLE, STOPPED, FAILED);
    private static final Set<ComponentState> RUNNING_STATES = Set.of(STARTING, STARTED, STOPPING);

    @GET()
    public Response getReadyState() {
        final var querier = GlobalState.INSTANCE.querier();

        if (querier == null) {
            // Service has not started yet
            return Response.serverError().build();
        }

        final var producerState = querier.getProducerState();
        final var streamRunnerState = querier.getStreamRunnerState();

        log.trace("Ready states - producer '{}' stream runner '{}' ", producerState, streamRunnerState);

        // Check if either Producer and StreamRunner has failed, or if they are BOTH in a not running state.
        if (producerState == FAILED || streamRunnerState == FAILED ||
                (NOT_RUNNING_STATES.contains(producerState) && NOT_RUNNING_STATES.contains(streamRunnerState))) {
            // One of the components has failed, or both are not running.
            return Response.serverError().build();
        }

        // Check if either producer of consumer is in a running state
        if (RUNNING_STATES.contains(producerState) || RUNNING_STATES.contains(streamRunnerState)) {
            // KSML has started, return HTTP Status code 204 (OK, No Content) if components
            return Response.noContent().build();
        } else {
            // KSML has started, return HTTP Status code 204 (OK, No Content) if components
            return Response.serverError().build();
        }
    }

}
