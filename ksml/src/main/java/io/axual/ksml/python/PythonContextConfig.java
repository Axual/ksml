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

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.jackson.Jacksonized;

@Getter
@ToString
@Builder
@Jacksonized
@JsonIgnoreProperties(ignoreUnknown = false)
@JsonClassDescription("Control Python execution security and permissions")
public class PythonContextConfig {
    @JsonProperty(value = "allowHostFileAccess", required = false, defaultValue = "false")
    @JsonPropertyDescription("Allow Python code to access host file system. Default is false.")
    private final boolean allowHostFileAccess;
    @JsonProperty(value = "allowHostSocketAccess", required = false, defaultValue = "false")
    @JsonPropertyDescription("Allow Python code to open network sockets. Default is false.")
    private final boolean allowHostSocketAccess;
    @JsonProperty(value = "allowNativeAccess", required = false, defaultValue = "false")
    @JsonPropertyDescription("Allow Graal native access / JNI. Default is false.")
    private final boolean allowNativeAccess;
    @JsonProperty(value = "allowCreateProcess", required = false, defaultValue = "false")
    @JsonPropertyDescription("Allow Python code to execute external processes. Default is false.")
    private final boolean allowCreateProcess;
    @JsonProperty(value = "allowCreateThread", required = false, defaultValue = "false")
    @JsonPropertyDescription("Allow Python code to create new Java threads. Default is false.")
    private final boolean allowCreateThread;
    @JsonProperty(value = "inheritEnvironmentVariables", required = false, defaultValue = "false")
    @JsonPropertyDescription("Inherit JVM process environment in Python context. Default is false.")
    private final boolean inheritEnvironmentVariables;
    @JsonProperty(value = "modulePath", required = false)
    @JsonPropertyDescription("Path to additional Python modules to be loaded. Default is empty, meaning 'no user modules'.")
    private final String modulePath;

    public PythonContextConfig(
            @JsonProperty(value = "allowHostFileAccess")
            boolean allowHostFileAccess,
            @JsonProperty(value = "allowHostSocketAccess")
            boolean allowHostSocketAccess,
            @JsonProperty(value = "allowNativeAccess")
            boolean allowNativeAccess,
            @JsonProperty(value = "allowCreateProcess")
            boolean allowCreateProcess,
            @JsonProperty(value = "allowCreateThread")
            boolean allowCreateThread,
            @JsonProperty(value = "inheritEnvironmentVariables")
            boolean inheritEnvironmentVariables,
            @JsonProperty(value = "modulePath")
            String modulePath) {
        this.allowHostFileAccess = allowHostFileAccess;
        this.allowHostSocketAccess = allowHostSocketAccess;
        this.allowNativeAccess = allowNativeAccess;
        this.allowCreateProcess = allowCreateProcess;
        this.allowCreateThread = allowCreateThread;
        this.inheritEnvironmentVariables = inheritEnvironmentVariables;
        this.modulePath = modulePath;
    }
}
