package io.axual.ksml.data.notation.xml;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library - XML
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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.MAP;

class XmlStringMapperTest {
    private final XmlStringMapper mapper = new XmlStringMapper("root", false);

    @Test
    @DisplayName("A simple element is parsed into a native map entry")
    void parsesSimpleElement() {
        final var result = mapper.fromString("<root><child>value</child></root>");

        assertThat(result).asInstanceOf(MAP).containsEntry("child", "value");
    }

    @Test
    @DisplayName("A namespace-prefixed element is parsed by its local name (namespace-aware reader)")
    void parsesNamespacePrefixedElementByLocalName() {
        // KSML's XML reader is namespace-aware: a declared prefix is resolved and the element is exposed
        // under its local name ("child"), not the qualified name ("ns:child"), and the namespace
        // declaration itself is not surfaced as data. A namespace-unaware reader would instead yield keys
        // "ns:child" and "xmlns:ns", changing the shape of every namespaced document.
        final var result = mapper.fromString("<root xmlns:ns=\"urn:ksml:test\"><ns:child>value</ns:child></root>");

        assertThat(result).asInstanceOf(MAP)
                .containsEntry("child", "value")
                .doesNotContainKey("ns:child")
                .doesNotContainKey("xmlns:ns");
    }
}
