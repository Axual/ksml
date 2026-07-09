package io.axual.ksml.user;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2024 Axual B.V.
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

import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.store.StateStores;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static io.axual.ksml.user.UserTestSupport.params;
import static io.axual.ksml.user.UserTestSupport.tags;
import static org.assertj.core.api.Assertions.assertThat;

class UserForeachActionTest {

    @Test
    void appliesActionInvokesUnderlyingCall() {
        // A forEach action must not declare a result type.
        final var invoked = new AtomicBoolean(false);
        final var function = new UserFunction("ns", "fn", params(2), null, (String[]) null) {
            @Override
            public DataObject call(StateStores stores, DataObject... parameters) {
                invoked.set(true);
                return null;
            }
        };
        final var action = new UserForeachAction(function, tags());

        action.apply(null, "key", "value");

        assertThat(invoked).isTrue();
    }
}
