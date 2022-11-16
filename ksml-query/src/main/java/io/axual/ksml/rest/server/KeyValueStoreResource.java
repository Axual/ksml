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

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.List;

import io.axual.ksml.rest.data.KeyValueBean;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Path("state/keyvalue")
public class KeyValueStoreResource extends StoreResource {
    private final ObjectMapper mapper = new ObjectMapper();

    /**
     * Get all the key-value pairs available in a store
     *
     * @param storeName store to query
     * @return A List of {@link KeyValueBean}s representing all the key-values in the provided
     * store
     */
    @GET()
    @Path("/{storeName}/all")
    @Produces(MediaType.APPLICATION_JSON)
    public List<KeyValueBean> getAll(@PathParam("storeName") final String storeName) {
        var result = getAllLocal(storeName);
        result.addAll(getAllRemote(storeName, "keyvalue"));
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
    public List<KeyValueBean> getAllLocal(@PathParam("storeName") final String storeName) {
        return getLocalRange(storeName, QueryableStoreTypes.keyValueStore(), ReadOnlyKeyValueStore::all).elements();
    }

    /**
     * Interface for fetching all store data
     *
     * @return StoreData from local and other remote stores (if needed)
     */
    @GET
    @Path("/{storeName}/get/{key}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public KeyValueBean getKey(@PathParam("storeName") final String storeName,
                               @PathParam("key") final String key) {
        KeyQueryMetadata metadataForKey = querier.queryMetadataForKey(storeName, key, new StringSerializer());

        if (metadataForKey.activeHost().host().equals(thisInstance.host()) && metadataForKey.activeHost().port() == thisInstance.port()) {
            log.info("Querying local store {} for key {}", storeName, key);
            var result = getKeyLocal(storeName, key);
            log.info("Store data from local store {}", result);
            return result;
        } else {
            log.info("Querying remote store {} for key {}", storeName, key);
            String url = "http://" + metadataForKey.activeHost() + ":" + metadataForKey.activeHost().port() + "/state/keyvalue/" + storeName + "/local/get/" + key;
            var result = restClient.getRemoteKeyValueBean(url, KeyValueBean.class);
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
    @Path("/{storeName}/local/get/{key}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public KeyValueBean getKeyLocal(@PathParam("storeName") final String storeName,
                                    @PathParam("key") final String key) {
        log.info("Querying local store {} for key {}", storeName, key);
        var stateStore = querier.store(
                StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore()));
        Object value = stateStore.get(key);
        log.info("Found value {}", value);
        return new KeyValueBean(key, value);
    }
}
