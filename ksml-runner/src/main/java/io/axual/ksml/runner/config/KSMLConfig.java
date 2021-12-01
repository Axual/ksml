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


import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import io.axual.ksml.runner.exception.KSMLRunnerConfigurationException;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
public class KSMLConfig {
    private static final String DEFAULT_HOSTNAME = "0.0.0.0";
    private static final String DEFAULT_PORT = "8080";

    private Boolean applicationServerEnabled;
    private String applicationServerHost;
    private String applicationServerPort;
    private String workingDirectory;
    private List<String> definitions;

    public String getApplicationServer() {
        if (applicationServerEnabled != null && applicationServerEnabled) {
            return (applicationServerHost != null ? applicationServerHost : DEFAULT_HOSTNAME)
                    + ":"
                    + (applicationServerPort != null ? applicationServerPort : DEFAULT_PORT);
        }
        return null;
    }

    public void validate() throws KSMLRunnerConfigurationException {
        if (workingDirectory == null) {
            throw new KSMLRunnerConfigurationException("workingDirectory", workingDirectory);
        }
        final var workingDirectoryPath = Paths.get(workingDirectory);
        if (Files.notExists(workingDirectoryPath) || !Files.isDirectory(workingDirectoryPath)) {
            throw new KSMLRunnerConfigurationException("workingDirectory", workingDirectory, "The provided path does not exists or is not a directory");
        }

        if (definitions == null || definitions.isEmpty()) {
            throw new KSMLRunnerConfigurationException("definitionFile", definitions, "At least one KSML definition file must be specified");
        }

        for (String definitionFile : definitions) {
            final var definitionFilePath = Paths.get(workingDirectory, definitionFile);
            if (Files.notExists(definitionFilePath) || !Files.isRegularFile(definitionFilePath)) {
                throw new KSMLRunnerConfigurationException("definitionFile", definitionFile, "The provided KSML definition file does not exists or is not a regular file");
            }
        }
    }
}
