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
public class RestClient implements AutoCloseable {
    private static Duration DEFAULT_TIMEOUT = Duration.ofSeconds(4);
    private Client client = null;

    private Client getRESTClient() {
        if (client == null) {
            client = ClientBuilder.newClient();
        }
        return client;
    }

    /**
     * Fetch remote KeyValueBeans using REST call
     *
     * @param url remote keyvalue endpoint URL
     * @return the StoreData
     */
    public KeyValueBeans getRemoteKeyValueBeans(String url) {
        return getRemoteKeyValueBeans(url, DEFAULT_TIMEOUT);
    }

    /**
     * Fetch remote KeyValueBeans using REST call
     *
     * @param url      remote keyvalue endpoint URL
     * @param duration duration for which to wait for response before giving up
     * @return the StoreData
     */
    public KeyValueBeans getRemoteKeyValueBeans(String url, Duration duration) {
        try {
            Future<KeyValueBeans> storeDataFuture = getRESTClient().target(url)
                    .request(MediaType.APPLICATION_JSON)
                    .async() //returns asap
                    .get(KeyValueBeans.class);
            return storeDataFuture.get(duration.toMillis(), TimeUnit.MILLISECONDS); //blocks until timeout
        } catch (Exception ex) {
            log.warn("Store data fetch from {} timed out", url);
            return new KeyValueBeans();
        }
    }

    /**
     * Fetch remote KeyValueBeans using REST call
     *
     * @param url remote keyvalue endpoint URL
     * @return the StoreData
     */
    public KeyValueBean getRemoteKeyValueBean(String url) {
        return getRemoteKeyValueBean(url, DEFAULT_TIMEOUT);
    }

    /**
     * Fetch remote KeyValueBeans using REST call
     *
     * @param url      remote keyvalue endpoint URL
     * @param duration duration for which to wait for response before giving up
     * @return the StoreData
     */
    public KeyValueBean getRemoteKeyValueBean(String url, Duration duration) {
        try {
            Future<KeyValueBean> storeDataFuture = getRESTClient().target(url)
                    .request(MediaType.APPLICATION_JSON)
                    .async() //returns asap
                    .get(KeyValueBean.class);
            return storeDataFuture.get(duration.toMillis(), TimeUnit.MILLISECONDS); //blocks until timeout
        } catch (Exception ex) {
            log.warn("Store data fetch from {} timed out", url);
            return null;
        }
    }

    @Override
    public void close() {
        if (client != null) {
            client.close();
        }
    }

    public static String getHostIPForDiscovery() {
        String host = HostDiscovery.discoverDocker();
        log.info(" Host IP {}", host);
        return host;
    }
}
