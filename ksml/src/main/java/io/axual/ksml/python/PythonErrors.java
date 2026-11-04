package io.axual.ksml.python;

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

import lombok.extern.slf4j.Slf4j;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Value;

/**
 * Raises Python TypeError or ValueError for caller errors, so Python {@code except Exception} catches them. Other
 * errors stay Java exceptions. Call from methods that Python calls, as {@code throw PythonErrors.typeError(...)}.
 */
@Slf4j
public final class PythonErrors {
    private PythonErrors() {
    }

    /** Raises a Python {@code TypeError}: the argument has the wrong kind, for example not a list. */
    public static RuntimeException typeError(String message) {
        return raise("TypeError", message);
    }

    /** Raises a Python {@code ValueError}: the argument has the right kind, but wrong content. */
    public static RuntimeException valueError(String message) {
        return raise("ValueError", message);
    }

    /** Raises a Python {@code ValueError} for a Java error, and logs the Java cause at debug level. */
    public static RuntimeException valueError(String message, Throwable cause) {
        log.debug("Raising ValueError in Python: {}", message, cause);
        return raise("ValueError", message);
    }

    /** The Python type name of a value, such as {@code str}. Never the value itself: it may be user data. */
    public static String typeName(Object value) {
        return builtin("type").execute(value).getMember("__name__").asString();
    }

    private static RuntimeException raise(String exceptionType, String message) {
        throw builtin(exceptionType).newInstance(message).throwException();
    }

    private static Value builtin(String name) {
        return Context.getCurrent().getBindings("python").getMember("__builtins__").getMember(name);
    }
}
