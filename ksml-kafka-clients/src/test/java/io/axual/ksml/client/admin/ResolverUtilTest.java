package io.axual.ksml.client.admin;

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

import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ResolverUtilTest {
    private final PrefixResolver resolver = new PrefixResolver();

    @Test
    @DisplayName("A topic ACL filter has its resource name resolved")
    void resolveTopicFilter() {
        final var filter = filter(ResourceType.TOPIC, "orders");

        final var resolved = ResolverUtil.resolve(filter, resolver, resolver);

        assertThat(resolved.patternFilter().name()).isEqualTo("tenant-orders");
    }

    @Test
    @DisplayName("A group ACL filter has its resource name resolved")
    void resolveGroupFilter() {
        final var filter = filter(ResourceType.GROUP, "group");

        final var resolved = ResolverUtil.resolve(filter, resolver, resolver);

        assertThat(resolved.patternFilter().name()).isEqualTo("tenant-group");
    }

    @Test
    @DisplayName("An ACL filter of another resource type is left unchanged")
    void resolveOtherFilter() {
        final var filter = filter(ResourceType.CLUSTER, "kafka-cluster");

        final var resolved = ResolverUtil.resolve(filter, resolver, resolver);

        assertThat(resolved.patternFilter().name()).isEqualTo("kafka-cluster");
    }

    @Test
    @DisplayName("ACL binding keys are unresolved per resource type")
    void unresolveBindingKeys() {
        final var bindings = List.of(
                binding(ResourceType.TOPIC, "tenant-orders"),
                binding(ResourceType.GROUP, "tenant-group"),
                binding(ResourceType.CLUSTER, "kafka-cluster"));

        final var unresolved = ResolverUtil.unresolveKeys(bindings, resolver, resolver);

        assertThat(unresolved).extracting(b -> b.pattern().name())
                .containsExactly("orders", "group", "kafka-cluster");
    }

    @Test
    @DisplayName("Map keys are unresolved with the topic resolver")
    void unresolveMapKeys() {
        final var map = Map.of("tenant-orders", 1, "tenant-payments", 2);

        final var unresolved = ResolverUtil.unresolveKeys(map, resolver);

        assertThat(unresolved).containsOnlyKeys("orders", "payments");
    }

    private static AclBindingFilter filter(ResourceType type, String name) {
        return new AclBindingFilter(
                new ResourcePatternFilter(type, name, PatternType.LITERAL),
                AccessControlEntryFilter.ANY);
    }

    private static AclBinding binding(ResourceType type, String name) {
        return new AclBinding(
                new ResourcePattern(type, name, PatternType.LITERAL),
                new AccessControlEntry("User:alice", "*", AclOperation.READ, AclPermissionType.ALLOW));
    }
}
