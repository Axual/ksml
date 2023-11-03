package io.axual.ksml.runner.config;

/*-
 * ========================LICENSE_START=================================
 * KSML Runner
 * %%
 * Copyright (C) 2021 Axual B.V.
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
import io.axual.ksml.runner.exception.RunnerConfigurationException;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

@Slf4j
@Data
public class KSMLConfig {
    private static final String DEFAULT_HOSTNAME = "0.0.0.0";
    private static final String DEFAULT_PORT = "8080";

    @JsonProperty("application.server.enabled")
    private Boolean applicationServerEnabled;
    @JsonProperty("application.server.host")
    private String applicationServerHost;
    @JsonProperty("application.server.port")
    private String applicationServerPort;
    @JsonProperty("working.directory")
    private String workingDirectory;
    @JsonProperty("error.handling")
    private KSMLErrorHandlingConfig errorHandling;

    @JsonProperty("config.directory")
    private String configurationDirectory;

    private List<String> definitions;

    public String getApplicationServer() {
        if (getApplicationServerEnabled()) {
            return getApplicationServerHost() + ":" + getApplicationServerPort();
        }
        return null;
    }

    public boolean getApplicationServerEnabled() {
        return applicationServerEnabled != null && applicationServerEnabled;
    }

    public String getApplicationServerHost() {
        if (getApplicationServerEnabled()) {
            return (applicationServerHost != null ? applicationServerHost : DEFAULT_HOSTNAME);
        }
        return null;
    }

    public Integer getApplicationServerPort() {
        if (getApplicationServerEnabled()) {
            return (applicationServerPort != null ? Integer.parseInt(applicationServerPort) : Integer.parseInt(DEFAULT_PORT));
        }
        return 0;
    }

    public String getConfigurationDirectory() {
        if (configurationDirectory == null) {
            return workingDirectory;
        }
        return configurationDirectory;
    }

    public KSMLErrorHandlingConfig getErrorHandlingConfig() {
        if (errorHandling == null) {
            return new KSMLErrorHandlingConfig();
        } else return errorHandling;
    }

    public void validate() throws RunnerConfigurationException {
        if (workingDirectory == null) {
            throw new RunnerConfigurationException("workingDirectory", workingDirectory);
        }

        final var workingDirectoryPath = Paths.get(workingDirectory);
        if (Files.notExists(workingDirectoryPath) || !Files.isDirectory(workingDirectoryPath)) {
            throw new RunnerConfigurationException("workingDirectory", workingDirectory, "The provided path does not exists or is not a directory");
        }

        if (configurationDirectory != null) {
            final var configPath = Paths.get(configurationDirectory);
            if (Files.notExists(configPath) || !Files.isDirectory(configPath)) {
                throw new RunnerConfigurationException("configurationDirectory", configurationDirectory, "The provided path does not exists or is not a directory");
            }
        }

        if (definitions == null || definitions.isEmpty()) {
            throw new RunnerConfigurationException("definitionFile", definitions, "At least one KSML definition file must be specified");
        }

        log.info("Using configuration directory: {}", getConfigurationDirectory());

        for (String definitionFile : definitions) {
            final var definitionFilePath = Paths.get(getConfigurationDirectory(), definitionFile);
            if (Files.notExists(definitionFilePath) || !Files.isRegularFile(definitionFilePath)) {
                throw new RunnerConfigurationException("definitionFile", definitionFilePath, "The provided KSML definition file does not exists or is not a regular file");
            }
        }
    }
}
