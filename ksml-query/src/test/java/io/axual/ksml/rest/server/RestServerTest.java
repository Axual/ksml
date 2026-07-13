package io.axual.ksml.rest.server;

/*-
 * ========================LICENSE_START=================================
 * KSML Queryable State Store
 * %%
 * Copyright (C) 2021 - 2026 Axual B.V.
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

import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.core.Response;
import org.apache.kafka.streams.state.HostInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
class RestServerTest {

    @Mock
    private KsmlQuerier querier;

    @AfterEach
    void clearGlobalState() {
        GlobalState.INSTANCE.set(null, null);
    }

    @Test
    @DisplayName("initGlobalQuerier publishes the querier and host info to the shared GlobalState")
    void publishesQuerierToGlobalState() throws Exception {
        // Port 0 lets the OS assign a free port at bind time, avoiding a flaky port grab.
        final var hostInfo = new HostInfo("localhost", 0);

        try (var server = new RestServer(hostInfo)) {
            server.initGlobalQuerier(querier);

            assertThat(GlobalState.INSTANCE.querier()).isSameAs(querier);
            assertThat(GlobalState.INSTANCE.hostInfo()).isSameAs(hostInfo);
        }
    }

    @Test
    @DisplayName("The server binds locally, serves its registered resources and shuts down cleanly")
    void servesRegisteredResources() throws Exception {
        // Port 0 lets the OS assign a free port at bind time; the real port is read back via boundPort().
        try (var server = new RestServer(new HostInfo("localhost", 0));
             var client = ClientBuilder.newClient()) {
            server.start();

            // No querier is registered, so the startup probe resource answers 500 - which still proves
            // the resource is wired up and the server is serving requests on its bound port.
            final var status = client.target("http://localhost:" + server.boundPort() + "/startup")
                    .request()
                    .get(Response.class)
                    .getStatus();

            assertThat(status).isEqualTo(500);
        }
    }
}
