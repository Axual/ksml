package io.axual.ksml.rest.server;

/*-
 * ========================LICENSE_START=================================
 * KSML Queryable State Store
 * %%
 * Copyright (C) 2021 - 2023 Axual B.V.
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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.jakarta.rs.json.JacksonJsonProvider;
import jakarta.ws.rs.core.UriBuilder;
import org.apache.kafka.streams.state.HostInfo;
import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.ServerConfiguration;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpContainer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ContainerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import java.io.IOException;

public class RestServer implements AutoCloseable {
    private static final String ROOT_RESOURCE_PATH = "";
    private final HostInfo hostInfo;
    private final HttpServer server;

    public RestServer(HostInfo hostInfo) {
        this.hostInfo = hostInfo;

        // create JsonProvider to provide custom ObjectMapper
        var mapper = new ObjectMapper();
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

        var provider = new JacksonJsonProvider();
        provider.setMapper(mapper);

        // configure REST service
        ResourceConfig rc = new ResourceConfig();
        rc.register(ReadyResource.class);
        rc.register(KeyValueStoreResource.class);
        rc.register(WindowedKeyValueStoreResource.class);
        rc.register(RestServerExceptionMapper.class);
        rc.register(provider);

        // create Grizzly instance and add handler
        HttpHandler handler = ContainerFactory.createContainer(
                GrizzlyHttpContainer.class, rc);
        var baseUri = UriBuilder.fromPath(ROOT_RESOURCE_PATH).scheme("http").host(hostInfo.host()).port(hostInfo.port()).build();
        server = GrizzlyHttpServerFactory.createHttpServer(baseUri);
        ServerConfiguration config = server.getServerConfiguration();
        config.addHttpHandler(handler, "/");
    }

    public String start() {
        try {
            server.start();
            return Utils.getHostIPForDiscovery();
        } catch (IOException e) {
            return null;
        }
    }

    public void initGlobalQuerier(KsmlQuerier ksmlQuerier) {
        GlobalState.INSTANCE.set(ksmlQuerier, hostInfo);
    }

    @Override
    public void close() {
        server.shutdownNow();
    }
}
