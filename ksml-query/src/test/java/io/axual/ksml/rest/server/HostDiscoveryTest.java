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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class HostDiscoveryTest {

    @Test
    @DisplayName("Local discovery always resolves to localhost")
    void discoverLocalReturnsLocalhost() {
        assertThat(HostDiscovery.discoverLocal()).isEqualTo("localhost");
    }

    @Test
    @DisplayName("Docker discovery returns the HOSTNAME environment variable")
    void discoverDockerReturnsHostnameEnv() {
        // Mutating the environment requires restricted JDK-internal access on modern JDKs, so this
        // verifies discoverDocker reflects the actual HOSTNAME value rather than injecting one.
        assertThat(HostDiscovery.discoverDocker()).isEqualTo(System.getenv("HOSTNAME"));
    }
}
