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

import java.time.Duration;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.core.MediaType;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class Utils {
    private Utils() {
    }

    private static Client REST_CLIENT = null;

    private static Client getRESTClient() {
        if (REST_CLIENT == null) {
            REST_CLIENT = ClientBuilder.newClient();
        }
        return REST_CLIENT;
    }

    /**
     * Fetch metrics using REST call
     *
     * @param url     remote metric endpoint URL
     * @param duration duration for which to wait for response before giving up
     * @return the StoreData
     */
    public static KeyValueBeans getRemoteStoreData(String url, Duration duration) {
        try {
            Future<KeyValueBeans> storeDataFuture = getRESTClient().target(url)
                    .request(MediaType.APPLICATION_JSON)
                    .async() //returns asap
                    .get(KeyValueBeans.class);

            return storeDataFuture.get(duration.toMillis(), TimeUnit.MILLISECONDS); //blocks until timeout
        } catch (Exception ex) {
            log.warn("Store data fetch from {} timed out", url);
            return KeyValueBeans.EMPTY();
        }
    }

    public static KeyValueBeans getRemoteStoreData(String url) {
        return getRESTClient().target(url)
                .request(MediaType.APPLICATION_JSON)
                .get(KeyValueBeans.class);
    }

    public static void closeRESTClient() {
        if (REST_CLIENT != null) {
            REST_CLIENT.close();
            log.info("REST Client closed");
        }
    }

    public static String getHostIPForDiscovery() {
        String host = HostDiscovery.discoverDocker();
        log.info(" Host IP {}", host);
        return host;
    }
}
