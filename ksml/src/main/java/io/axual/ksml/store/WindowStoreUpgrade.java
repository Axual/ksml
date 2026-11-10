package io.axual.ksml.store;

/*-
 * ========================LICENSE_START=================================
 * KSML
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

import io.axual.ksml.definition.StateStoreDefinition;

import java.nio.file.Path;
import java.util.Collection;

/**
 * Clears window store state that KSML 1.x wrote in a format this version can't read, once per application state
 * directory, so Kafka Streams restores it from the changelog (design D7).
 */
public final class WindowStoreUpgrade {
    /** Marks an application state directory as checked. */
    public static final String MARKER_FILE = ".ksml-store-format";

    private WindowStoreUpgrade() {
    }

    /**
     * @param applicationDirectory the state directory of the application: {@code <state.dir>/<application.id>}
     * @param stores               the stores built for Python functions
     */
    public static void prepare(Path applicationDirectory, Collection<? extends StateStoreDefinition> stores) {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
