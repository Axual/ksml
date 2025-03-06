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


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

@Slf4j
@JsonIgnoreProperties(ignoreUnknown = true)
@Data
@Builder(toBuilder = true)
@Jacksonized
public class PrometheusConfig {
    private static final String DEFAULT_HOSTNAME = "0.0.0.0";
    private static final String DEFAULT_PORT = "9999";
    private static final String DEFAULT_CONFIG_RESOURCE = "prometheus/default_config.yaml";
    private static File defaultConfigFile;

    private boolean enabled;
    private String host;
    private String port;
    private Path configFile;

    public String getHost() {
        if (!enabled) return null;
        return (host != null ? host : DEFAULT_HOSTNAME);
    }

    public Integer getPort() {
        if (!enabled) return null;
        return (port != null ? Integer.parseInt(port) : Integer.parseInt(DEFAULT_PORT));
    }

    private static synchronized File getDefaultConfigFile() {
        if (defaultConfigFile == null || !defaultConfigFile.exists()) {
            File tmpFile = null;
            try {
                log.info("Loading default config file: {}", defaultConfigFile);
                final var resourceUrl = PrometheusConfig.class.getClassLoader().getResource(DEFAULT_CONFIG_RESOURCE);
                if (resourceUrl != null) {
                    tmpFile = File.createTempFile("KSML-Prometheus-Default-Config", ".yaml");
                    tmpFile.deleteOnExit();
                    IOUtils.copy(resourceUrl, tmpFile);
                } else {
                    log.warn("Could not load prometheus config from {}", DEFAULT_CONFIG_RESOURCE);
                }
            } catch (IOException e) {
                log.info("Could not create temporary prometheus config file");
            }
            defaultConfigFile = tmpFile;
        }
        return defaultConfigFile;
    }

    public File getConfigFile() {
        if (!enabled) return null;
        return (configFile != null) ? configFile.toFile() : getDefaultConfigFile();
    }
}
