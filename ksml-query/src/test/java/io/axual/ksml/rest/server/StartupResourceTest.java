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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.stream.Stream;

import static io.axual.ksml.rest.server.ComponentState.CREATED;
import static io.axual.ksml.rest.server.ComponentState.FAILED;
import static io.axual.ksml.rest.server.ComponentState.NOT_APPLICABLE;
import static io.axual.ksml.rest.server.ComponentState.STARTED;
import static io.axual.ksml.rest.server.ComponentState.STARTING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Named.named;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StartupResourceTest {

    private static final int OK_NO_CONTENT = 204;
    private static final int SERVER_ERROR = 500;

    private final StartupResource resource = new StartupResource();

    @Mock
    private KsmlQuerier querier;

    @AfterEach
    void clearGlobalState() {
        GlobalState.INSTANCE.set(null, null);
    }

    @Test
    @DisplayName("Returns 500 while no querier is available yet (still starting up)")
    void serverErrorWhenNoQuerier() {
        GlobalState.INSTANCE.set(null, new HostInfo("localhost", 8080));

        assertThat(resource.getStartupState().getStatus()).isEqualTo(SERVER_ERROR);
    }

    static Stream<Arguments> startupStates() {
        return Stream.of(
                arguments(named("both not applicable -> not started", NOT_APPLICABLE), NOT_APPLICABLE, SERVER_ERROR),
                arguments(named("producer starting, stream started -> started", STARTING), STARTED, OK_NO_CONTENT),
                arguments(named("both started -> started", STARTED), STARTED, OK_NO_CONTENT),
                arguments(named("producer created -> not started", CREATED), STARTED, SERVER_ERROR),
                arguments(named("producer failed -> not started", FAILED), STARTED, SERVER_ERROR)
        );
    }

    @ParameterizedTest
    @MethodSource("startupStates")
    @DisplayName("Maps producer/stream states to the startup HTTP status")
    void mapsStateToStatus(ComponentState producerState, ComponentState streamState, int expectedStatus) {
        GlobalState.INSTANCE.set(querier, new HostInfo("localhost", 8080));
        when(querier.getProducerState()).thenReturn(producerState);
        when(querier.getStreamRunnerState()).thenReturn(streamState);

        assertThat(resource.getStartupState().getStatus()).isEqualTo(expectedStatus);
    }
}
