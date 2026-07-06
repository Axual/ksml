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
import io.axual.ksml.rest.data.WindowedKeyValueBeans;
import jakarta.ws.rs.client.AsyncInvoker;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.MediaType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import jakarta.ws.rs.ServiceUnavailableException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RestClientTest {

    private static final String URL = "http://remote:9090/state/keyValue/store/local/all";

    @Mock
    private Client client;
    @Mock
    private WebTarget target;
    @Mock
    private Invocation.Builder builder;
    @Mock
    private AsyncInvoker asyncInvoker;

    private MockedStatic<ClientBuilder> clientBuilder;
    private RestClient restClient;

    @BeforeEach
    void setup() {
        clientBuilder = mockStatic(ClientBuilder.class);
        clientBuilder.when(ClientBuilder::newClient).thenReturn(client);
        restClient = new RestClient();
    }

    @AfterEach
    void tearDown() {
        clientBuilder.close();
    }

    /** Stubs the async REST chain so that {@code asyncInvoker.get(SomeClass.class)} returns the given future. */
    private void stubChainReturning(Future<?> future) {
        when(client.target(anyString())).thenReturn(target);
        when(target.request(MediaType.APPLICATION_JSON)).thenReturn(builder);
        when(builder.async()).thenReturn(asyncInvoker);
        doReturn(future).when(asyncInvoker).get(any(Class.class));
    }

    @Test
    @DisplayName("getRemoteKeyValueBeans returns the fetched beans on success")
    void keyValueBeansSuccess() throws Exception {
        final var beans = new KeyValueBeans();
        final var future = mockFutureReturning(beans);
        stubChainReturning(future);

        assertThat(restClient.getRemoteKeyValueBeans(URL)).isSameAs(beans);
    }

    @Test
    @DisplayName("getRemoteKeyValueBeans returns empty beans when the call times out")
    void keyValueBeansTimeout() throws Exception {
        stubChainReturning(mockFutureThrowing(new TimeoutException()));

        assertThat(restClient.getRemoteKeyValueBeans(URL).elements()).isEmpty();
    }

    @Test
    @DisplayName("getRemoteKeyValueBeans returns empty beans when the call fails")
    void keyValueBeansExecutionError() throws Exception {
        stubChainReturning(mockFutureThrowing(new ExecutionException(new RuntimeException("boom"))));

        assertThat(restClient.getRemoteKeyValueBeans(URL).elements()).isEmpty();
    }

    @Test
    @DisplayName("getRemoteKeyValueBeans restores the interrupt flag when interrupted")
    void keyValueBeansInterrupted() throws Exception {
        stubChainReturning(mockFutureThrowing(new InterruptedException()));

        restClient.getRemoteKeyValueBeans(URL);

        assertThat(Thread.currentThread().isInterrupted()).isTrue();
        Thread.interrupted(); // clear for other tests
    }

    @Test
    @DisplayName("getRemoteWindowedKeyValueBeans returns the fetched beans on success")
    void windowedBeansSuccess() throws Exception {
        final var beans = new WindowedKeyValueBeans();
        stubChainReturning(mockFutureReturning(beans));

        assertThat(restClient.getRemoteWindowedKeyValueBeans(URL)).isSameAs(beans);
    }

    @Test
    @DisplayName("getRemoteWindowedKeyValueBeans returns empty beans when the call times out")
    void windowedBeansTimeout() throws Exception {
        stubChainReturning(mockFutureThrowing(new TimeoutException()));

        assertThat(restClient.getRemoteWindowedKeyValueBeans(URL).elements()).isEmpty();
    }

    @Test
    @DisplayName("getRemoteWindowedKeyValueBeans restores the interrupt flag when interrupted")
    void windowedBeansInterrupted() throws Exception {
        stubChainReturning(mockFutureThrowing(new InterruptedException()));

        restClient.getRemoteWindowedKeyValueBeans(URL);

        assertThat(Thread.currentThread().isInterrupted()).isTrue();
        Thread.interrupted(); // clear for other tests
    }

    @Test
    @DisplayName("getRemoteKeyValueBean returns the fetched bean on success")
    void singleBeanSuccess() throws Exception {
        final var bean = new KeyValueBean(new DataString("k"), new DataString("v"));
        stubChainReturning(mockFutureReturning(bean));

        assertThat(restClient.getRemoteKeyValueBean(URL, KeyValueBean.class)).isSameAs(bean);
    }

    @Test
    @DisplayName("getRemoteKeyValueBean throws ServiceUnavailableException when the call times out")
    void singleBeanTimeout() throws Exception {
        stubChainReturning(mockFutureThrowing(new TimeoutException()));

        assertThatThrownBy(() -> restClient.getRemoteKeyValueBean(URL, KeyValueBean.class))
                .isInstanceOf(ServiceUnavailableException.class);
    }

    @Test
    @DisplayName("getRemoteKeyValueBean throws ServiceUnavailableException when the call fails")
    void singleBeanExecutionError() throws Exception {
        stubChainReturning(mockFutureThrowing(new ExecutionException(new RuntimeException("boom"))));

        assertThatThrownBy(() -> restClient.getRemoteKeyValueBean(URL, KeyValueBean.class))
                .isInstanceOf(ServiceUnavailableException.class);
    }

    @Test
    @DisplayName("getRemoteKeyValueBean restores the interrupt flag and throws when interrupted")
    void singleBeanInterrupted() throws Exception {
        stubChainReturning(mockFutureThrowing(new InterruptedException()));

        assertThatThrownBy(() -> restClient.getRemoteKeyValueBean(URL, KeyValueBean.class))
                .isInstanceOf(ServiceUnavailableException.class);
        assertThat(Thread.currentThread().isInterrupted()).isTrue();
        Thread.interrupted(); // clear for other tests
    }

    @Test
    @DisplayName("close closes the underlying REST client once it has been created")
    void closeClosesClient() throws Exception {
        stubChainReturning(mockFutureReturning(new KeyValueBeans()));
        restClient.getRemoteKeyValueBeans(URL); // forces lazy creation of the JAX-RS client

        restClient.close();

        verify(client).close();
    }

    @SuppressWarnings("unchecked")
    private static <T> Future<T> mockFutureReturning(T value) throws Exception {
        final Future<T> future = mock(Future.class);
        when(future.get(anyLong(), any(TimeUnit.class))).thenReturn(value);
        return future;
    }

    @SuppressWarnings("unchecked")
    private static Future<Object> mockFutureThrowing(Exception toThrow) throws Exception {
        final Future<Object> future = mock(Future.class);
        when(future.get(anyLong(), any(TimeUnit.class))).thenThrow(toThrow);
        return future;
    }
}
