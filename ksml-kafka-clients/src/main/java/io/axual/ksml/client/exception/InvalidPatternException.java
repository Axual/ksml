package io.axual.ksml.client.exception;

/*-
 * ========================LICENSE_START=================================
 * KSML Kafka clients
 * %%
 * Copyright (C) 2021 - 2025 Axual B.V.
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

import lombok.Getter;

/**
 * Exception indicating an invalid pattern was provided. Formats the exception message to contain
 * the provided pattern and reason.
 */
@Getter
public class InvalidPatternException extends RuntimeException {
    private final String pattern;
    private final String reason;

    /**
     * Construct the exception by providing the offending pattern and reason for failure.
     * Formats the exception message to contain both the provided pattern and reason.
     *
     * @param pattern The pattern causing the exception
     * @param reason  The reason why the pattern is invalid
     */
    public InvalidPatternException(String pattern, String reason) {
        super(formatMessage(pattern, reason));
        this.pattern = pattern;
        this.reason = reason;
    }

    private static String formatMessage(String pattern, String reason) {
        return """
                Invalid Pattern: %s
                Reason: %s
                """.formatted(pattern, reason);
    }
}
