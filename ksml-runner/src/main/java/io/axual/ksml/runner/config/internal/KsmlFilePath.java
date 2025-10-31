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

/**
 * Concrete {@link KsmlFileOrDefinition} representing a reference to a KSML file on disk.
 * The JSON representation is a simple string containing the (relative or absolute) file path.
 */
public final class KsmlFilePath implements KsmlFileOrDefinition {
    /** The file path as provided in the configuration. */
    private final String value;

    /**
     * Create a file-path variant for a KSML definition reference.
     * @param value path to the file (relative or absolute)
     */
    public KsmlFilePath(String value) {
        this.value = value;
    }

    @Override
    public String getValue() {
        return value;
    }
}
