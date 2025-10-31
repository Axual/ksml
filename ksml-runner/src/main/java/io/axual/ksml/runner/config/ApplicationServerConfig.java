package io.axual.ksml.runner.config;

/*-
 * ========================LICENSE_START=================================
 * KSML Runner
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


import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;

import java.util.Optional;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
@JsonIgnoreProperties(ignoreUnknown = false)
@JsonClassDescription("Controls if a REST API for health checks and querying state stores is available, and on which host it's listening")
@NoArgsConstructor
public class ApplicationServerConfig {
    private static final String DEFAULT_HOSTNAME = "0.0.0.0";
    private static final Integer DEFAULT_PORT = 8080;

    @JsonProperty(value = "enabled", required = true)
    @JsonPropertyDescription("Toggle to activate the creation of the application server. Default is false")
    private boolean enabled = false;
    @JsonProperty(value = "host", required = false)
    @JsonPropertyDescription("Determines on which hostname/ip address the application server listener is created. Default is IP address for all networks '0.0.0.0'")
    private String host = DEFAULT_HOSTNAME;
    @JsonProperty(value = "port", required = false)
    @JsonPropertyDescription("Determines on which port the application server is listening. Default is 8080")
    @Min(1)
    @Max(65535)
    private Integer port = DEFAULT_PORT;

    @JsonIgnore
    public String getApplicationServer() {
        if (!enabled) return null;
        return getHost() + ":" + getPort();
    }

    public String getHost() {
        if (!enabled) return null;
        return Optional.ofNullable(host).orElse(DEFAULT_HOSTNAME);
    }

    public Integer getPort() {
        if (!enabled) return null;
        return Optional.ofNullable(port).orElse(DEFAULT_PORT);
    }
}
