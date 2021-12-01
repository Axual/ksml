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

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreType;

import java.util.function.Function;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StoreResource implements AutoCloseable {
    protected final StreamsQuerier querier = GlobalState.getInstance().querier();
    protected final HostInfo thisInstance = GlobalState.getInstance().hostInfo();
    protected final RestClient restClient = new RestClient();

    protected <T, K, V> KeyValueBeans getLocalRange(final String storeName,
                                                    final QueryableStoreType<T> storeQueryParameters,
                                                    final Function<T, KeyValueIterator<K, V>> rangeFunction) {
        log.info("Querying remote stores....");
        // Get the KeyValue Store
        final StreamsQuerier querier = GlobalState.getInstance().querier();

        final var store = querier.store(
                StoreQueryParameters.fromNameAndType(storeName, storeQueryParameters));
        final var result = new KeyValueBeans();
        // Apply the function, i.e., query the store
        final var range = rangeFunction.apply(store);

        // Convert the results
        while (range.hasNext()) {
            final KeyValue<K, V> next = range.next();
            result.add(new KeyValueBean(next.key, next.value));
        }

        log.info("Complete store state {}", result);
        return result;
    }

    protected KeyValueBeans getAllRemote(String storeName, String stateSubPath) {
        log.info("Querying remote stores....");
        var result = new KeyValueBeans();
        querier.allMetadataForStore(storeName)
                .stream()
                .filter(sm -> !(sm.host().equals(thisInstance.host()) && sm.port() == thisInstance.port())) //only query remote node stores
                .forEach(remoteInstance -> {
                    String url = "http://" + remoteInstance.host() + ":" + remoteInstance.port() + "/state/" + stateSubPath + "/" + storeName + "/local/all";
                    log.info("Fetching remote store at {}:{}", remoteInstance.host(), remoteInstance.port());
                    KeyValueBeans remoteResult = restClient.getRemoteKeyValueBeans(url);
                    log.info("Data from remote store at {}:{} == {}", remoteInstance.host(), remoteInstance.port(), remoteResult);
                    result.add(remoteResult);
                });

        log.info("Complete store state {}", result);
        return result;
    }

    @Override
    public void close() {
        restClient.close();
    }
}
