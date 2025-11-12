package io.axual.ksml.data.exception;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2023 Axual B.V.
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

/**
 * Base runtime exception for KSML with standardized, readable messages.
 *
 * <p>Subclasses supply an activity label (for example, "data", "schema", or "validation")
 * which is combined with the message into the format:
 * <pre>KSML {activity} error: {message}</pre>
 * This keeps diagnostics consistent across the module.</p>
 */
public abstract class BaseException extends RuntimeException {
    private static final String PREFIX = "KSML ";
    private static final String POSTFIX = " error: ";

    /**
     * Create an exception with a standardized prefix based on the activity.
     *
     * @param activity logical subsystem where the error occurred (used in the prefix)
     * @param message  human-readable details of the failure
     */
    protected BaseException(String activity, String message) {
        super(PREFIX + activity + POSTFIX + message);
    }

    /**
     * Create an exception with a standardized prefix and a cause.
     *
     * @param activity logical subsystem where the error occurred (used in the prefix)
     * @param message  human-readable details of the failure
     * @param t        underlying cause
     */
    protected BaseException(String activity, String message, Throwable t) {
        super(PREFIX + activity + POSTFIX + message, t);
    }
}
