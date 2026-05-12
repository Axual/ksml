package io.axual.ksml.testrunner;

/*-
 * ========================LICENSE_START=================================
 * KSML Test Runner
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

/**
 * A single test message with key, value, and optional timestamp.
 *
 * @param key       the message key
 * @param value     the message value (typically a map for structured data)
 * @param timestamp optional epoch milliseconds timestamp; null means auto-advancing time
 */
@JsonSchema(description = "A single test message with key, value, and optional timestamp")
public record TestMessage(
        @JsonSchema(description = "Message key", required = true, examples = {"sensor-1", "user-42"})
        Object key,

        @JsonSchema(description = "Message value (string or structured data)", required = true)
        Object value,

        @JsonSchema(description = "Optional timestamp in epoch milliseconds", examples = {"1709200000000"})
        Long timestamp
) {
}
