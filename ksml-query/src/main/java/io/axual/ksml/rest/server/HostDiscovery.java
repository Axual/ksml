package io.axual.ksml.rest.server;

/*-
 * ========================LICENSE_START=================================
 * KSML Queryable State Store
 * %%
 * Copyright (C) 2021 Axual B.V.
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

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HostDiscovery {
    private static final String LOCALHOST = "localhost";

    private HostDiscovery() {
    }

    public static String discoverLocal() {
        return LOCALHOST;
    }

    public static String discoverDocker() {
        log.info("Docker based host discovery..");
        String dockerHostName = System.getenv("HOSTNAME");

        log.info("Docker container host name - " + dockerHostName);
        return dockerHostName;
    }
}
