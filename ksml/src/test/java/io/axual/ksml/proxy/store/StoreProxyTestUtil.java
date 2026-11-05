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

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.PolyglotException;

import java.util.List;

/** Helpers for the store proxy tests that run Python code. */
final class StoreProxyTestUtil {
    private StoreProxyTestUtil() {
    }

    /** Python values of every kind, written as Python literals. */
    static final List<String> PYTHON_VALUES = List.of("'s'", "5", "10**30", "1.5", "True", "None", "{'a': 1}", "[1, 2]");

    /** Runs Python code and returns "ok", or the type of the Python or Java error it raised. */
    static String outcome(Context context, String code) {
        try {
            context.eval("python", code);
            return "ok";
        } catch (PolyglotException e) {
            return e.isHostException() ? e.asHostException().getClass().getSimpleName() : e.getMessage().split(":")[0];
        }
    }

    /** The class and the text of an object, so objects from two separate calls can be compared. */
    static String describe(Object object) {
        return object == null ? "null" : object.getClass().getSimpleName() + ":" + object;
    }
}
