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



import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.util.ServiceLoader;

import io.axual.ksml.runner.backend.Backend;
import io.axual.ksml.runner.backend.BackendConfig;
import io.axual.ksml.runner.backend.BackendProvider;
import io.axual.ksml.runner.exception.KSMLRunnerConfigurationException;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
public class KSMLRunnerConfig {
    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    private KSMLConfig ksml;
    private KSMLRunnerBackendConfig backend;

    public void validate() throws KSMLRunnerConfigurationException {
        if (ksml == null) {
            throw new KSMLRunnerConfigurationException("ksml", ksml);
        }

        ksml.validate();

        if (backend == null) {
            throw new KSMLRunnerConfigurationException("backend", backend);
        }

        backend.validate();
    }

    public Backend getConfiguredBackend() throws JsonProcessingException {
        validate();

        ServiceLoader<BackendProvider> loader = ServiceLoader.load(BackendProvider.class);
        if (log.isInfoEnabled()) {
            loader.forEach(pr -> log.info("Found provider {} for type {}", pr.getClass().getName(), pr.getType()));
        }

        final String type = backend.getType();
        final JsonNode config = backend.getConfig();

        BackendProvider<?> provider = loader.stream()
                .map(ServiceLoader.Provider::get)
                .filter(pr -> pr.getType().equals(type) || pr.getClass().getName().equals(type))
                .findFirst()
                .orElseThrow(() -> new RuntimeException(String.format("No BackendProvider for type '%s' found", type)));

        final BackendConfig backendConfig = mapper.readValue(mapper.writeValueAsString(config), provider.getConfigClass());
        backendConfig.validate();

        return provider.create(ksml, backendConfig);
    }
}
