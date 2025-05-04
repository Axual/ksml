package io.axual.ksml.python;

/*-
 * ========================LICENSE_START=================================
 * KSML
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

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;

@Getter
@Builder
@Jacksonized
public class PythonContextConfig {
    @JsonProperty("allowHostFileAccess")
    @Builder.Default
    private boolean allowHostFileAccess = false;

    @JsonProperty("allowHostSocketAccess")
    @Builder.Default
    private boolean allowHostSocketAccess = false;

    @JsonProperty("allowNativeAccess")
    @Builder.Default
    private boolean allowNativeAccess = false;

    @JsonProperty("allowCreateProcess")
    @Builder.Default
    private boolean allowCreateProcess = false;

    @JsonProperty("allowCreateThread")
    @Builder.Default
    private boolean allowCreateThread = false;

    @JsonProperty("inheritEnvironmentVariables")
    @Builder.Default
    private boolean inheritEnvironmentVariables = false;
}
