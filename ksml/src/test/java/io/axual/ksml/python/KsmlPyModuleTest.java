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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * KSML ships the ksml module and loads it into every Python context.
 */
class KsmlPyModuleTest {

    @Test
    @DisplayName("the types of the ksml module can be imported without a module path")
    void runtimeImports() {
        try (var pythonContext = new PythonContext(PythonContextConfig.builder().build())) {
            final var result = pythonContext.context().eval("python", """
                    from ksml import PythonLogger, MetricsBridge, KeyValueStore, SessionStore, WindowStore
                    from ksml import TimestampedKeyValueStore, TimestampedWindowStore, VersionedKeyValueStore
                    from ksml import StoreIterator, KSMLStoreIterator, ProcessorContext, KSMLProcessorContext
                    StoreIterator is KSMLStoreIterator and ProcessorContext is KSMLProcessorContext
                    """);
            assertThat(result.asBoolean()).isTrue();
        }
    }

    @Test
    @DisplayName("a ksml.py in the module path does not hide the ksml module")
    void moduleInModulePathIsHidden(@TempDir Path moduleDir) throws IOException {
        Files.writeString(moduleDir.resolve("ksml.py"), "raise ImportError('the copy in the module path was loaded')\n");
        final var config = PythonContextConfig.builder().modulePath(moduleDir.toString()).build();
        try (var pythonContext = new PythonContext(config)) {
            final var result = pythonContext.context().eval("python", """
                    from ksml import KSMLStoreIterator
                    KSMLStoreIterator.__name__
                    """);
            assertThat(result.asString()).isEqualTo("KSMLStoreIterator");
        }
    }

    @Test
    @DisplayName("Python contexts each get their own ksml module")
    void modulePerContext() {
        try (var first = new PythonContext(PythonContextConfig.builder().build());
             var second = new PythonContext(PythonContextConfig.builder().build())) {
            first.context().eval("python", "import ksml\nksml.marker = 1");
            assertThat(second.context().eval("python", "import ksml\nhasattr(ksml, 'marker')").asBoolean()).isFalse();
        }
    }
}
