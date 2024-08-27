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

@Slf4j
@Path("ready")
public class ReadyResource {
    @GET()
    public Response getReadyState() {
        final var querier = GlobalState.INSTANCE.querier();
        if (querier == null) {
            // Service has not started yet
            return Response.serverError().build();
        }

        final var producerRunning = switch (querier.getProducerState()) {
            case STARTING, STARTED, STOPPING, STOPPED -> true;
            default -> false;
        };
        final var streamsRunning = switch (querier.getStreamRunnerState()) {
            case STARTING, STARTED, STOPPING, STOPPED -> true;
            default -> false;
        };

        if (streamsRunning && producerRunning) {
            return Response.noContent().build();
        } else {
            return Response.serverError().build();
        }
    }
}
