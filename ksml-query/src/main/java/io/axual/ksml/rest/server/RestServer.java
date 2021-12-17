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

import org.apache.kafka.streams.state.HostInfo;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.moxy.json.MoxyJsonFeature;
import org.glassfish.jersey.server.ResourceConfig;

import java.io.IOException;
import java.net.URI;

import jakarta.ws.rs.core.UriBuilder;

public class RestServer implements AutoCloseable {
    private static final String ROOT_RESOURCE_PATH = "";
    private final HostInfo hostInfo;
    private final HttpServer server;

    public RestServer(HostInfo hostInfo) {
        this.hostInfo = hostInfo;
        //Start Grizzly container
        URI baseUri = UriBuilder.fromPath(ROOT_RESOURCE_PATH).scheme("http").host(hostInfo.host()).port(hostInfo.port()).build();
        ResourceConfig config = new ResourceConfig(KeyValueStoreResource.class, WindowedStoreResource.class).register(MoxyJsonFeature.class);
        server = GrizzlyHttpServerFactory.createHttpServer(baseUri, config);
    }

    public String start(StreamsQuerier querier) {
        try {
            GlobalState.INSTANCE.set(querier, hostInfo);
            server.start();
            return Utils.getHostIPForDiscovery();
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public void close() {
        server.shutdownNow();
    }
}
