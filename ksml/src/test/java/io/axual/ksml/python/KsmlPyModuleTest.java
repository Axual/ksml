package io.axual.ksml.python;

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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The ksml.py module from the docs must import at runtime, not only in the IDE.
 */
class KsmlPyModuleTest {

    @ParameterizedTest
    @ValueSource(strings = {"/ksml.py", "/docs-examples/use-cases/fraud-detection/ksml.py"})
    @DisplayName("the types of ksml.py can be imported at runtime")
    void runtimeImports(String resource, @TempDir Path moduleDir) throws IOException {
        try (var module = Objects.requireNonNull(getClass().getResourceAsStream(resource), resource)) {
            Files.copy(module, moduleDir.resolve("ksml.py"));
        }
        final var config = PythonContextConfig.builder().modulePath(moduleDir.toString()).build();
        try (var pythonContext = new PythonContext(config)) {
            final var result = pythonContext.context().eval("python", """
                    from ksml import PythonLogger, MetricsBridge, KeyValueStore, SessionStore, WindowStore
                    from ksml import TimestampedKeyValueStore, TimestampedWindowStore, VersionedKeyValueStore
                    from ksml import StoreIterator
                    StoreIterator is not None
                    """);
            assertThat(result.asBoolean()).isTrue();
        }
    }
}
