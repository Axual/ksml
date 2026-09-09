package io.axual.ksml.proxy.store;

/*-
 * ========================LICENSE_START=================================
 * KSML
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
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.python.PythonContext;
import io.axual.ksml.python.PythonContextConfig;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

/** Confirms a value read back from a key/value store survives {@code copy.deepcopy()}. */
@ExtendWith(MockitoExtension.class)
class KeyValueStoreProxyDeepcopyTest {
    @Mock
    private KeyValueStore<Object, Object> delegate;

    @Test
    void deepcopyOnAStateStoreReadResult() {
        var nested = new DataStruct();
        nested.put("city", new DataString("Amsterdam"));
        when(delegate.get("sensor1")).thenReturn(nested);

        var proxy = new KeyValueStoreProxy(delegate);

        try (var pythonContext = new PythonContext(PythonContextConfig.builder().build())) {
            var bindings = pythonContext.context().getBindings("python");
            // Call .get() from Python, like a real pipeline does, not from plain Java
            bindings.putMember("store", proxy);

            var isDict = pythonContext.context().eval("python", "type(store.get('sensor1')) is dict");
            System.out.println("type(store.get(key)) is dict -> " + isDict);
            assertThat(isDict.asBoolean()).isTrue();

            var deepcopyResult = pythonContext.context().eval("python", """
                    import copy
                    value = store.get('sensor1')
                    copy.deepcopy(value)
                    'OK'
                    """);
            System.out.println("copy.deepcopy(store.get(key)) -> " + deepcopyResult);
            assertThat(deepcopyResult.asString()).isEqualTo("OK");
        }
    }
}
