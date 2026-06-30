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

import org.apache.kafka.streams.state.HostInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.net.ServerSocket;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

@ExtendWith(MockitoExtension.class)
class RestServerTest {

    @Mock
    private KsmlQuerier querier;

    @AfterEach
    void clearGlobalState() {
        GlobalState.INSTANCE.set(null, null);
    }

    private static int freePort() throws Exception {
        try (var socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    @Test
    @DisplayName("The server registers its resources, binds locally and shuts down cleanly")
    void lifecycleStartsAndStops() throws Exception {
        final var hostInfo = new HostInfo("localhost", freePort());

        assertThatCode(() -> {
            try (var server = new RestServer(hostInfo)) {
                server.initGlobalQuerier(querier);
                assertThat(GlobalState.INSTANCE.querier()).isSameAs(querier);
                assertThat(GlobalState.INSTANCE.hostInfo()).isSameAs(hostInfo);
            }
        }).doesNotThrowAnyException();
    }
}
