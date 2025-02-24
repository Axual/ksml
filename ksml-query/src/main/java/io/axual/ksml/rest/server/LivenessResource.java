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

import static io.axual.ksml.rest.server.ComponentState.*;

@Slf4j(topic = "ksml.rest.service.live")
@Path("live")
public class LivenessResource {

    @GET()
    public Response getLivenessState() {
        final var querier = GlobalState.INSTANCE.querier();

        if (querier == null) {
            // Service has not started yet
            log.trace("KSML Not Alive - No querier available, still in startup");
            return Response.serverError().build();
        }

        final var producerState = querier.getProducerState();
        final var streamRunnerState = querier.getStreamRunnerState();

        // Misconfiguration
        if( producerState == NOT_APPLICABLE && streamRunnerState == NOT_APPLICABLE ) {
            log.trace("KSML Not Alive - Both producerState and streamRunnerState are disabled");
            return Response.serverError().build();
        }

        if( producerState == FAILED || streamRunnerState == FAILED || streamRunnerState == STOPPED){
            log.trace("KSML Not Alive - producer state '{}' stream runner state '{}' ", producerState, streamRunnerState);
            return Response.serverError().build();
        } else {
            // KSML is alive, return HTTP Status code 204 (OK, No Content) if components
            log.trace("KSML Alive - producer state '{}' stream runner state '{}' ", producerState, streamRunnerState);
            return Response.noContent().build();
        }
    }

}
