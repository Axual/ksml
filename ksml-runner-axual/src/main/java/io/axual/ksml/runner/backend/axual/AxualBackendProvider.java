package io.axual.ksml.runner.backend.axual;

/*-
 * ========================LICENSE_START=================================
 * KSML Runner for Axual
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



import com.google.auto.service.AutoService;

import io.axual.ksml.runner.backend.Backend;
import io.axual.ksml.runner.backend.BackendConfig;
import io.axual.ksml.runner.backend.BackendProvider;
import io.axual.ksml.runner.config.KSMLRunnerKSMLConfig;

@AutoService(BackendProvider.class)
public class AxualBackendProvider implements BackendProvider<AxualBackendConfig> {
    private static final String PROVIDER_TYPE = "axual";

    @Override
    public String getType() {
        return PROVIDER_TYPE;
    }

    @Override
    public Backend create(KSMLRunnerKSMLConfig KSMLRunnerKsmlConfig, BackendConfig backendConfig) {
        return new AxualBackend(KSMLRunnerKsmlConfig, (AxualBackendConfig) backendConfig);
    }

    @Override
    public Class<AxualBackendConfig> getConfigClass() {
        return AxualBackendConfig.class;
    }

}
