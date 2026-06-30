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
import io.axual.ksml.rest.data.WindowedKeyValueBean;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Field;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class WindowedKeyValueStoreResourceTest {

    private static final HostInfo LOCAL = new HostInfo("localhost", 8080);
    private static final String STORE = "windowStore";

    @Mock
    private KsmlQuerier querier;
    @Mock
    private ReadOnlyWindowStore<Object, Object> store;
    @Mock
    private KeyValueIterator<Windowed<Object>, Object> iterator;
    @Mock
    private RestClient restClient;
    @Mock
    private StreamsMetadata streamsMetadata;

    private WindowedKeyValueStoreResource resource;

    @BeforeEach
    void setup() {
        GlobalState.INSTANCE.set(querier, LOCAL);
        resource = new WindowedKeyValueStoreResource();
    }

    @AfterEach
    void clearGlobalState() {
        GlobalState.INSTANCE.set(null, null);
    }

    private static Windowed<Object> windowedKey(String key) {
        return new Windowed<>(key, new TimeWindow(0L, 100L));
    }

    private static void injectRestClient(StoreResource resource, RestClient client) throws Exception {
        final Field field = StoreResource.class.getDeclaredField("restClient");
        field.setAccessible(true);
        field.set(resource, client);
    }

    @Test
    @DisplayName("getAllLocal converts every windowed entry in the local store to a bean")
    void getAllLocalConvertsWindowedEntries() {
        when(querier.store(any())).thenReturn(store);
        when(store.all()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true, false);
        when(iterator.next()).thenReturn(KeyValue.pair(windowedKey("k1"), "v1"));

        final List<WindowedKeyValueBean> result = resource.getAllLocal(STORE);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).key()).isEqualTo(new DataString("k1"));
        assertThat(result.get(0).value()).isEqualTo(new DataString("v1"));
        assertThat(result.get(0).window().start()).isZero();
        assertThat(result.get(0).window().end()).isEqualTo(100L);
    }

    @Test
    @DisplayName("getKeyLocal returns the latest windowed value fetched from the local store")
    void getKeyLocalReturnsLatestValue() {
        when(querier.store(any())).thenReturn(store);
        when(store.fetch(any(), any(), any(Instant.class), any(Instant.class))).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true, false);
        when(iterator.next()).thenReturn(KeyValue.pair(windowedKey("k1"), "v1"));

        final WindowedKeyValueBean result = resource.getKeyLocal(STORE, "k1", 50L);

        assertThat(result).isNotNull();
        assertThat(result.value()).isEqualTo(new DataString("v1"));
    }

    @Test
    @DisplayName("getKeyLocal returns null when the store has no matching window")
    void getKeyLocalReturnsNullWhenEmpty() {
        when(querier.store(any())).thenReturn(store);
        when(store.fetch(any(), any(), any(Instant.class), any(Instant.class))).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(false);

        assertThat(resource.getKeyLocal(STORE, "k1", 50L)).isNull();
    }

    @Test
    @DisplayName("getKey queries the local store when the key's active host is this instance")
    void getKeyRoutesToLocalStore() {
        when(querier.queryMetadataForKey(any(), any(), any())).thenReturn(new KeyQueryMetadata(LOCAL, java.util.Set.of(), 0));
        when(querier.store(any())).thenReturn(store);
        when(store.fetch(any(), any(), any(Instant.class), any(Instant.class))).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true, false);
        when(iterator.next()).thenReturn(KeyValue.pair(windowedKey("k1"), "v1"));

        final WindowedKeyValueBean result = resource.getKey(STORE, "k1", 50L);

        assertThat(result.value()).isEqualTo(new DataString("v1"));
    }

    @Test
    @DisplayName("getAll combines local entries and skips the local instance among store metadata")
    void getAllSkipsLocalInstanceForRemoteFetch() {
        when(querier.store(any())).thenReturn(store);
        when(store.all()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true, false);
        when(iterator.next()).thenReturn(KeyValue.pair(windowedKey("k1"), "v1"));
        when(streamsMetadata.host()).thenReturn(LOCAL.host());
        when(streamsMetadata.port()).thenReturn(LOCAL.port());
        when(querier.allMetadataForStore(STORE)).thenReturn(List.of(streamsMetadata));

        assertThat(resource.getAll(STORE)).hasSize(1);
    }

    @Test
    @DisplayName("getKey delegates to the remote REST client when the key lives on another instance")
    void getKeyRoutesToRemoteInstance() throws Exception {
        injectRestClient(resource, restClient);
        final var remoteBean = new WindowedKeyValueBean(new TimeWindow(0L, 100L), new DataString("k1"), new DataString("remote"));
        when(querier.queryMetadataForKey(any(), any(), any())).thenReturn(new KeyQueryMetadata(new HostInfo("other", 9090), java.util.Set.of(), 0));
        when(restClient.getRemoteKeyValueBean(any(), any())).thenReturn(remoteBean);

        final WindowedKeyValueBean result = resource.getKey(STORE, "k1", 50L);

        assertThat(result).isSameAs(remoteBean);
    }
}
