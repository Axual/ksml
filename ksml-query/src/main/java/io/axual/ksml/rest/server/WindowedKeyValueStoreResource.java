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

import io.axual.ksml.rest.data.KeyValueBean;
import io.axual.ksml.rest.data.WindowedKeyValueBean;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;

import java.time.Instant;
import java.util.List;

@Slf4j
@Path("state/windowed")
public class WindowedKeyValueStoreResource extends StoreResource {
    @GET()
    @Path("/{storeName}/all")
    @Produces(MediaType.APPLICATION_JSON)
    public List<WindowedKeyValueBean> getAll(@PathParam("storeName") final String storeName) {
        var result = getAllLocal(storeName);
        log.info("Querying remote stores....");
        querier().allMetadataForStore(storeName)
                .stream()
                .filter(sm -> !(sm.host().equals(thisInstance.host()) && sm.port() == thisInstance.port())) //only query remote node stores
                .forEach(remoteInstance -> {
                    String url = "http://" + remoteInstance.host() + ":" + remoteInstance.port() + "/state/windowed/" + storeName + "/local/all";
                    log.info("Fetching remote store at {}:{}", remoteInstance.host(), remoteInstance.port());
                    List<WindowedKeyValueBean> remoteResult = restClient.getRemoteWindowedKeyValueBeans(url).elements();
                    log.info("Data from remote store at {}:{} == {}", remoteInstance.host(), remoteInstance.port(), remoteResult);
                    result.addAll(remoteResult);
                });

        log.info("Complete store state {}", result);
        return result;
    }

    /**
     * Get all the local key-value pairs available in a store
     *
     * @param storeName store to query
     * @return A List of {@link KeyValueBean}s representing all the local key-values in the provided
     * store
     */
    @GET()
    @Path("/{storeName}/local/all")
    @Produces(MediaType.APPLICATION_JSON)
    public List<WindowedKeyValueBean> getAllLocal(@PathParam("storeName") final String storeName) {
        return getLocalWindowRange(storeName, QueryableStoreTypes.windowStore(), ReadOnlyWindowStore::all).elements();
    }

    /**
     * Interface for fetching all store data
     *
     * @return StoreData from local and other remote stores (if needed)
     */
    @GET
    @Path("/{storeName}/get/{key}/{timestamp}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public WindowedKeyValueBean getKey(@PathParam("storeName") final String storeName,
                                       @PathParam("key") final String key,
                                       @PathParam("timestamp") final Long timestamp) {
        KeyQueryMetadata metadataForKey = querier().queryMetadataForKey(storeName, key, new StringSerializer());

        if (metadataForKey.activeHost().host().equals(thisInstance.host()) && metadataForKey.activeHost().port() == thisInstance.port()) {
            log.info("Querying local store {} for key {}", storeName, key);
            var result = getKeyLocal(storeName, key, timestamp);
            log.info("Store data from local store {}", result);
            return result;
        } else {
            log.info("Querying remote store {} for key {}", storeName, key);
            String url = "http://" + metadataForKey.activeHost() + ":" + metadataForKey.activeHost().port() + "/state/keyvalue/" + storeName + "/local/get/" + key;
            var result = restClient.getRemoteKeyValueBean(url, WindowedKeyValueBean.class);
            log.info("Store data from remote store at {} == {}", url, result);
            return result;
        }
    }

    /**
     * Interface for fetching all store data
     *
     * @return StoreData from local and other remote stores (if needed)
     */
    @GET
    @Path("/{storeName}/local/get/{key}/{timestamp}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public WindowedKeyValueBean getKeyLocal(@PathParam("storeName") final String storeName,
                                            @PathParam("key") final String key,
                                            @PathParam("timestamp") final Long timestamp) {
        log.info("Querying local store {} for key {}", storeName, key);
        var stateStore = getStore(
                StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.windowStore()));
        var iterator = stateStore.fetch(key, key, Instant.ofEpochMilli(timestamp), Instant.ofEpochMilli(timestamp));
        KeyValue<Windowed<Object>, Object> latest = null;
        while (iterator.hasNext()) {
            latest = iterator.next();
        }
        var result = latest != null
                ? new WindowedKeyValueBean(latest.key.window(), NATIVE_MAPPER.toDataObject(latest.key.key()), NATIVE_MAPPER.toDataObject(latest.value))
                : null;
        log.info("Found value {}", result);
        return result;
    }
}
