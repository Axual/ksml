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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.axual.ksml.rest.data.KeyValueBean;
import io.axual.ksml.rest.data.KeyValueBeans;
import io.axual.ksml.rest.data.WindowedKeyValueBeans;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.core.MediaType;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RestClient implements AutoCloseable {
    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(4);
    private static final String INTERRUPTED_MESSAGE = "Store data fetch from {} was interrupted";
    private static final String TIMEOUT_MESSAGE = "Store data fetch from {} timed out";
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
     * @param url remote keyvalue endpoint URL
     * @return the StoreData
     */
    public WindowedKeyValueBeans getRemoteWindowedKeyValueBeans(String url) {
        return getRemoteWindowKeyValueBeans(url, DEFAULT_TIMEOUT);
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
        } catch (InterruptedException | ExecutionException e) {
            log.warn(INTERRUPTED_MESSAGE, url);
            Thread.currentThread().interrupt();
        } catch (TimeoutException e) {
            log.warn(TIMEOUT_MESSAGE, url);
        }

        return new KeyValueBeans();
    }

    /**
     * Fetch remote KeyValueBeans using REST call
     *
     * @param url      remote keyvalue endpoint URL
     * @param duration duration for which to wait for response before giving up
     * @return the StoreData
     */
    public WindowedKeyValueBeans getRemoteWindowKeyValueBeans(String url, Duration duration) {
        try {
            Future<WindowedKeyValueBeans> storeDataFuture = getRESTClient().target(url)
                    .request(MediaType.APPLICATION_JSON)
                    .async() //returns asap
                    .get(WindowedKeyValueBeans.class);
            return storeDataFuture.get(duration.toMillis(), TimeUnit.MILLISECONDS); //blocks until timeout
        } catch (InterruptedException | ExecutionException e) {
            log.warn(INTERRUPTED_MESSAGE, url);
            Thread.currentThread().interrupt();
        } catch (TimeoutException e) {
            log.warn(TIMEOUT_MESSAGE, url);
        }
        return new WindowedKeyValueBeans();
    }

    /**
     * Fetch remote KeyValueBeans using REST call
     *
     * @param url remote keyvalue endpoint URL
     * @return the StoreData
     */
    public <T extends KeyValueBean> T getRemoteKeyValueBean(String url, Class<T> resultClass) {
        return getRemoteKeyValueBean(url, resultClass, DEFAULT_TIMEOUT);
    }

    /**
     * Fetch remote KeyValueBeans using REST call
     *
     * @param url      remote keyvalue endpoint URL
     * @param duration duration for which to wait for response before giving up
     * @return the StoreData
     */
    public <T extends KeyValueBean> T getRemoteKeyValueBean(String url, Class<T> resultClass, Duration duration) {
        try {
            Future<T> storeDataFuture = getRESTClient().target(url)
                    .request(MediaType.APPLICATION_JSON)
                    .async() //returns asap
                    .get(resultClass);
            return storeDataFuture.get(duration.toMillis(), TimeUnit.MILLISECONDS); //blocks until timeout
        } catch (InterruptedException | ExecutionException e) {
            log.warn(INTERRUPTED_MESSAGE, url);
            Thread.currentThread().interrupt();
        } catch (TimeoutException e) {
            log.warn(TIMEOUT_MESSAGE, url);
        }
        return null;
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
