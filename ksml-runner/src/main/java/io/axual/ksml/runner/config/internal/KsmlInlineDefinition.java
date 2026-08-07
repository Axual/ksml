package io.axual.ksml.runner.config.internal;

/*-
 * ========================LICENSE_START=================================
 * KSML Runner
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

import tools.jackson.databind.node.ObjectNode;

/**
 * Concrete {@link KsmlFileOrDefinition} representing an inline KSML definition.
 * The JSON representation is the object itself embedded directly in the configuration file.
 *
 * @param value the JSON content of the inline definition
 */
public record KsmlInlineDefinition(ObjectNode value) implements KsmlFileOrDefinition {
    @Override
    public ObjectNode getValue() {
        return value;
    }
}
