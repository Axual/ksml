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

import io.axual.ksml.client.testutil.PrefixResolver;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises the collection default methods of {@link GroupResolver} through a prefixing test double.
 */
class GroupResolverTest {
    private static final String PREFIX = PrefixResolver.PREFIX;

    private final GroupResolver resolver = new PrefixResolver();

    @Test
    @DisplayName("Resolve of a group collection prefixes every group id")
    void resolveCollection() {
        assertThat(resolver.resolve(List.of("a", "b"))).containsExactlyInAnyOrder(PREFIX + "a", PREFIX + "b");
    }

    @Test
    @DisplayName("Unresolve of a group collection drops unresolvable group ids")
    void unresolveCollection() {
        assertThat(resolver.unresolve(List.of(PREFIX + "a", "unprefixed"))).containsExactly("a");
    }

    @Test
    @DisplayName("Null group collections resolve to empty sets")
    void nullCollections() {
        assertThat(resolver.resolve((java.util.Collection<String>) null)).isEmpty();
        assertThat(resolver.unresolve((java.util.Collection<String>) null)).isEmpty();
    }
}
