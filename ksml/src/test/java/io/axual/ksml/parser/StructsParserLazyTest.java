package io.axual.ksml.parser;

/*-
 * ========================LICENSE_START=================================
 * KSML
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

import tools.jackson.databind.JsonNode;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.generator.YAMLObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

// Covers the memoize-once and fatal-error-wrapping contracts StructsParser.lazy()
// must preserve from DefinitionParser's historic parse()/schemas() implementation.
class StructsParserLazyTest {

    private ParseNode nodeOf(String yaml) throws Exception {
        final var root = YAMLObjectMapper.INSTANCE.readValue(yaml, JsonNode.class);
        return ParseNode.fromRoot(root, "test");
    }

    @Test
    void buildsDelegateExactlyOnce() throws Exception {
        final var buildCount = new AtomicInteger();
        final StructsParser<String> lazy = StructsParser.lazy(() -> {
            buildCount.incrementAndGet();
            return StructsParser.of(node -> "value", List.<StructSchema>of());
        });

        final var node = nodeOf("value: 1");
        lazy.parse(node);
        lazy.schemas();
        lazy.parse(node);

        assertThat(buildCount.get()).isEqualTo(1);
    }

    @Test
    void lazy_wrapsParseExceptionsAsFatalError() throws Exception {
        final StructsParser<String> lazy = StructsParser.lazy(() -> StructsParser.of(node -> {
            throw new IllegalStateException("boom");
        }, List.<StructSchema>of()));

        final var node = nodeOf("value: 1");
        assertThatThrownBy(() -> lazy.parse(node))
                .isExactlyInstanceOf(RuntimeException.class)
                .hasMessage("boom")
                .hasNoCause();
    }

    @Test
    void lazy_doesNotWrapConstructionExceptions() {
        final StructsParser<String> lazy = StructsParser.lazy(() -> {
            throw new IllegalStateException("construction failed");
        });

        assertThatThrownBy(lazy::schemas)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("construction failed");
    }
}
