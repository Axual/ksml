package io.axual.ksml.client.resolving;

/*-
 * ========================LICENSE_START=================================
 * KSML Kafka clients
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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises the default methods of {@link TransactionalIdResolver} through a prefixing test double.
 */
class TransactionalIdResolverTest {
    private static final String PREFIX = "resolved-";

    private final TransactionalIdResolver resolver = new TransactionalIdResolver() {
        @Override
        public String resolve(String name) {
            return name == null ? null : PREFIX + name;
        }

        @Override
        public String unresolve(String name) {
            if (name == null || !name.startsWith(PREFIX)) return null;
            return name.substring(PREFIX.length());
        }
    };

    @Test
    @DisplayName("Resolve and unresolve of a single transactional id delegate to resolve/unresolve")
    void singleTransactionalId() {
        assertThat(resolver.resolveTransactionalId("tx")).isEqualTo(PREFIX + "tx");
        assertThat(resolver.unresolveTransactionalId(PREFIX + "tx")).isEqualTo("tx");
    }

    @Test
    @DisplayName("Resolve of a transactional id collection prefixes every id")
    void resolveCollection() {
        assertThat(resolver.resolveTransactionalIds(List.of("a", "b")))
                .containsExactlyInAnyOrder(PREFIX + "a", PREFIX + "b");
    }

    @Test
    @DisplayName("Unresolve of a transactional id collection drops unresolvable ids")
    void unresolveCollection() {
        assertThat(resolver.unresolveTransactionalIds(List.of(PREFIX + "a", "unprefixed"))).containsExactly("a");
    }

    @Test
    @DisplayName("Null transactional id collections resolve to empty sets")
    void nullCollections() {
        assertThat(resolver.resolveTransactionalIds(null)).isEmpty();
        assertThat(resolver.unresolveTransactionalIds(null)).isEmpty();
    }
}
