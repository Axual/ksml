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
import jakarta.annotation.Nonnull;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

@Slf4j
@JsonIgnoreProperties(ignoreUnknown = false)
@JsonClassDescription("Controls on which hostname and port the Prometheus metrics are accessible, and which prometheus exporter configuration file to use")
@Data
@AllArgsConstructor
@NoArgsConstructor
public class PrometheusConfig {
    /** Default bind address for the exporter (all interfaces). */
    private static final String DEFAULT_HOSTNAME = "0.0.0.0";
    /** Default TCP port for the exporter endpoint. */
    private static final Integer DEFAULT_PORT = 9999;
    /** Classpath resource containing the fallback exporter configuration. */
    private static final String DEFAULT_CONFIG_RESOURCE = "prometheus/default_config.yaml";
    /** Lazily materialized temp file containing the default exporter config. */
    private static File defaultConfigFile;

    /**
     * Copy constructor for defensive copying in composite configs.
     *
     * @param config the original configuration
     */
    public PrometheusConfig(@Nonnull PrometheusConfig config) {
        this.enabled = config.enabled;
        this.host = config.host;
        this.port = config.port;
        this.configFile = config.configFile;
    }

    @JsonProperty(value = "enabled", required = true)
    @JsonPropertyDescription("Toggle to activate the creation Prometheus metrics exporter. Default is false")
    private boolean enabled = false;

    @NotBlank
    @JsonProperty(value = "host", required = false)
    @JsonPropertyDescription("Determines on which hostname/ip address the Prometheus metrics exporter listener is created. Default is IP address for all networks '0.0.0.0'")
    private String host = DEFAULT_HOSTNAME;

    @Min(1)
    @Max(65535)
    @JsonProperty(value = "port", required = false)
    @JsonPropertyDescription("Determines on which port the Prometheus metrics exporter is listening. Default is 9999")
    private Integer port = DEFAULT_PORT;

    /** Optional path to an explicit Prometheus exporter configuration file. */
    @JsonProperty(value = "configFile", required = false)
    @JsonPropertyDescription("Path to a Prometheus JMX Exporter configuration file, containing metrics exposure and naming rules. If not set an internal definition is used.")
    private String configFile;

    /**
     * Resolve the bind host. Returns null when the exporter is disabled so callers can interpret
     * "disabled" without extra flags.
     */
    public String getHost() {
        if (!enabled) return null;
        return Optional.ofNullable(host).orElse(DEFAULT_HOSTNAME);
    }

    /**
     * Resolve the bind port. Returns null when the exporter is disabled.
     */
    public Integer getPort() {
        if (!enabled) return null;
        return Optional.ofNullable(port).orElse(DEFAULT_PORT);
    }

    /**
     * Lazily create and cache a temporary file containing the default exporter configuration
     * from the classpath resource. This ensures downstream code can work with a File API.
     */
    private static synchronized File getDefaultConfigFile() {
        if (defaultConfigFile == null || !defaultConfigFile.exists()) {
            File tmpFile = null;
            try {
                log.info("Loading default config file from internal resources: {}", DEFAULT_CONFIG_RESOURCE);
                final var resourceUrl = PrometheusConfig.class.getClassLoader().getResource(DEFAULT_CONFIG_RESOURCE);
                if (resourceUrl != null) {
                    // Create a temp file and copy the resource into it so components expecting a File can use it
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

    /**
     * Provide the exporter configuration file to use. When disabled, returns null. When no explicit
     * file is configured, falls back to a temporary file containing the default config from the classpath.
     */
    @JsonIgnore
    public File getConfigFile() {
        if (!enabled) return null;
        if (configFile == null) {
            return getDefaultConfigFile();
        }
        return new File(configFile);
    }
}
