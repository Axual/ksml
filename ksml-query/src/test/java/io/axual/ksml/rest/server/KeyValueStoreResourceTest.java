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

import io.axual.ksml.data.object.DataString;
import io.axual.ksml.rest.data.KeyValueBean;
import io.axual.ksml.rest.data.KeyValueBeans;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.ServiceUnavailableException;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.errors.UnknownStateStoreException;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KeyValueStoreResourceTest {

    private static final HostInfo LOCAL = new HostInfo("localhost", 8080);
    private static final HostInfo REMOTE = new HostInfo("other", 9090);
    private static final String STORE = "myStore";

    @Mock
    private KsmlQuerier querier;
    @Mock
    private ReadOnlyKeyValueStore<Object, Object> store;
    @Mock
    private KeyValueIterator<Object, Object> iterator;
    @Mock
    private StreamsMetadata streamsMetadata;

    @BeforeEach
    void setup() {
        GlobalState.INSTANCE.set(querier, LOCAL);
    }

    @AfterEach
    void clearGlobalState() {
        GlobalState.INSTANCE.set(null, null);
    }

    @Test
    @DisplayName("getAllLocal converts every key/value in the local store to a bean")
    void getAllLocalConvertsStoreEntries() {
        when(querier.store(any())).thenReturn(store);
        when(store.all()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true, true, false);
        when(iterator.next()).thenReturn(KeyValue.pair("k1", "v1"), KeyValue.pair("k2", "v2"));

        final var result = new KeyValueStoreResource().getAllLocal(STORE);

        assertThat(result).hasSize(2);
        final var first = result.get(0);
        assertThat(first.key()).isEqualTo(new DataString("k1"));
        assertThat(first.value()).isEqualTo(new DataString("v1"));
    }

    @Test
    @DisplayName("getKeyLocal reads a single value from the local store")
    void getKeyLocalReadsValue() {
        when(querier.store(any())).thenReturn(store);
        when(store.get("k1")).thenReturn("v1");

        final var result = new KeyValueStoreResource().getKeyLocal(STORE, "k1");

        assertThat(result.key()).isEqualTo(new DataString("k1"));
        assertThat(result.value()).isEqualTo(new DataString("v1"));
    }

    @Test
    @DisplayName("getKey queries the local store when the key's active host is this instance")
    void getKeyRoutesToLocalStore() {
        when(querier.queryMetadataForKey(any(), any(), any())).thenReturn(metadataOnHost(LOCAL));
        when(querier.store(any())).thenReturn(store);
        when(store.get("k1")).thenReturn("v1");

        final var result = new KeyValueStoreResource().getKey(STORE, "k1");

        assertThat(result.value()).isEqualTo(new DataString("v1"));
    }

    @Test
    @DisplayName("getKey delegates to the remote REST client, hitting the key's local endpoint on the owning instance")
    void getKeyRoutesToRemoteInstance() {
        final var remoteBean = new KeyValueBean(new DataString("k1"), new DataString("remote"));
        final var url = ArgumentCaptor.forClass(String.class);
        when(querier.queryMetadataForKey(any(), any(), any())).thenReturn(metadataOnHost(REMOTE));

        try (var restClients = mockConstruction(RestClient.class,
                (mock, ctx) -> when(mock.getRemoteKeyValueBean(url.capture(), any())).thenReturn(remoteBean))) {
            final var result = new KeyValueStoreResource().getKey(STORE, "k1");

            assertThat(result).isSameAs(remoteBean);
            assertThat(url.getValue()).isEqualTo("http://other:9090/state/keyValue/" + STORE + "/local/get/k1");
        }
    }

    @Test
    @DisplayName("getAll combines local entries and skips the local instance among store metadata")
    void getAllSkipsLocalInstanceForRemoteFetch() {
        when(querier.store(any())).thenReturn(store);
        when(store.all()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true, false);
        when(iterator.next()).thenReturn(KeyValue.pair("k1", "v1"));
        // Only the local instance is registered, so the remote-fetch filter skips it.
        when(streamsMetadata.host()).thenReturn(LOCAL.host());
        when(streamsMetadata.port()).thenReturn(LOCAL.port());
        when(querier.allMetadataForStore(STORE)).thenReturn(List.of(streamsMetadata));

        final var result = new KeyValueStoreResource().getAll(STORE);

        assertThat(result).hasSize(1);
    }

    @Test
    @DisplayName("getAll merges local entries with the beans fetched from each remote instance")
    void getAllMergesRemoteEntries() {
        when(querier.store(any())).thenReturn(store);
        when(store.all()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true, false);
        when(iterator.next()).thenReturn(KeyValue.pair("k1", "v1"));
        when(streamsMetadata.host()).thenReturn(REMOTE.host());
        when(streamsMetadata.port()).thenReturn(REMOTE.port());
        when(querier.allMetadataForStore(STORE)).thenReturn(List.of(streamsMetadata));
        final var remoteBeans = new KeyValueBeans().add(new DataString("k2"), new DataString("v2"));
        final var url = ArgumentCaptor.forClass(String.class);

        try (var restClients = mockConstruction(RestClient.class,
                (mock, ctx) -> when(mock.getRemoteKeyValueBeans(url.capture())).thenReturn(remoteBeans))) {
            final var result = new KeyValueStoreResource().getAll(STORE);

            assertThat(result).hasSize(2);
            assertThat(url.getValue()).isEqualTo("http://other:9090/state/keyValue/" + STORE + "/local/all");
        }
    }

    @Test
    @DisplayName("A missing store is reported as a 404 NotFoundException")
    void unknownStoreBecomesNotFound() {
        when(querier.store(any())).thenThrow(new UnknownStateStoreException("nope"));

        final var resource = new KeyValueStoreResource();
        assertThatThrownBy(() -> resource.getAllLocal(STORE)).isInstanceOf(NotFoundException.class);
    }

    @Test
    @DisplayName("When no querier is available the resource reports a 503 ServiceUnavailableException")
    void noQuerierBecomesServiceUnavailable() {
        GlobalState.INSTANCE.set(null, LOCAL);

        final var resource = new KeyValueStoreResource();
        assertThatThrownBy(() -> resource.getAllLocal(STORE)).isInstanceOf(ServiceUnavailableException.class);
    }

    private static KeyQueryMetadata metadataOnHost(HostInfo host) {
        return new KeyQueryMetadata(host, Set.of(), 0);
    }
}
