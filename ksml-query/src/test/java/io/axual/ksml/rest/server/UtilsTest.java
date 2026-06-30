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

import io.axual.ksml.rest.data.KeyValueBeans;
import jakarta.ws.rs.client.AsyncInvoker;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.MediaType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class UtilsTest {

    private static final String URL = "http://remote:9090/state/keyvalue/store/local/all";

    @Mock
    private Client client;
    @Mock
    private WebTarget target;
    @Mock
    private Invocation.Builder builder;
    @Mock
    private AsyncInvoker asyncInvoker;

    @BeforeEach
    void injectClient() throws Exception {
        setStaticClient(client);
    }

    @AfterEach
    void clearClient() throws Exception {
        setStaticClient(null);
    }

    private static void setStaticClient(Client value) throws Exception {
        final Field field = Utils.class.getDeclaredField("restClient");
        field.setAccessible(true);
        field.set(null, value);
    }

    private void stubAsyncChain(Future<?> future) {
        when(client.target(anyString())).thenReturn(target);
        when(target.request(MediaType.APPLICATION_JSON)).thenReturn(builder);
        when(builder.async()).thenReturn(asyncInvoker);
        doReturn(future).when(asyncInvoker).get(any(Class.class));
    }

    @Test
    @DisplayName("getRemoteStoreData (with timeout) returns the fetched beans on success")
    void timedFetchSuccess() throws Exception {
        final var beans = new KeyValueBeans();
        final Future<KeyValueBeans> future = mockFuture();
        when(future.get(anyLong(), any(TimeUnit.class))).thenReturn(beans);
        stubAsyncChain(future);

        assertThat(Utils.getRemoteStoreData(URL, Duration.ofMillis(10))).isSameAs(beans);
    }

    @Test
    @DisplayName("getRemoteStoreData (with timeout) returns empty beans when the call times out")
    void timedFetchTimeout() throws Exception {
        final Future<KeyValueBeans> future = mockFuture();
        when(future.get(anyLong(), any(TimeUnit.class))).thenThrow(new TimeoutException());
        stubAsyncChain(future);

        assertThat(Utils.getRemoteStoreData(URL, Duration.ofMillis(10)).elements()).isEmpty();
    }

    @Test
    @DisplayName("getRemoteStoreData (with timeout) returns empty beans when the call fails")
    void timedFetchExecutionError() throws Exception {
        final Future<KeyValueBeans> future = mockFuture();
        when(future.get(anyLong(), any(TimeUnit.class))).thenThrow(new ExecutionException(new RuntimeException("boom")));
        stubAsyncChain(future);

        assertThat(Utils.getRemoteStoreData(URL, Duration.ofMillis(10)).elements()).isEmpty();
    }

    @Test
    @DisplayName("getRemoteStoreData (no timeout) fetches synchronously")
    void synchronousFetch() {
        final var beans = new KeyValueBeans();
        when(client.target(anyString())).thenReturn(target);
        when(target.request(MediaType.APPLICATION_JSON)).thenReturn(builder);
        when(builder.get(KeyValueBeans.class)).thenReturn(beans);

        assertThat(Utils.getRemoteStoreData(URL)).isSameAs(beans);
    }

    @Test
    @DisplayName("closeRESTClient closes the underlying REST client")
    void closeClosesClient() {
        Utils.closeRESTClient();

        verify(client).close();
    }

    @Test
    @DisplayName("getHostIPForDiscovery reflects the HOSTNAME environment variable")
    void hostIpReflectsHostname() {
        assertThat(Utils.getHostIPForDiscovery()).isEqualTo(System.getenv("HOSTNAME"));
    }

    @SuppressWarnings("unchecked")
    private static <T> Future<T> mockFuture() {
        return org.mockito.Mockito.mock(Future.class);
    }
}
