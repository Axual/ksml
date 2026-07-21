package io.axual.ksml.runner.config;

/*-
 * ========================LICENSE_START=================================
 * KSML Runner
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

import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.dataformat.yaml.YAMLMapper;

/**
 * Single source of the Jackson mapper used to read the KSML runner configuration (ksml-runner.yaml).
 *
 * <p>It enables {@link DeserializationFeature#FAIL_ON_UNKNOWN_PROPERTIES} so an unknown or misspelled key
 * fails at startup instead of being silently ignored. Jackson 3 defaults this feature to off and the
 * config records use {@code @JsonIgnoreProperties(ignoreUnknown = false)}, so it must be enabled here to
 * keep the strict validation KSML had on Jackson 2. Production and tests use this same instance so the
 * tests always validate against the real parsing behavior.</p>
 */
public final class RunnerConfigMapper {
    private RunnerConfigMapper() {
    }

    /** Thread-safe, immutable once built; safe to share. */
    public static final ObjectMapper INSTANCE = YAMLMapper.builder()
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();
}
