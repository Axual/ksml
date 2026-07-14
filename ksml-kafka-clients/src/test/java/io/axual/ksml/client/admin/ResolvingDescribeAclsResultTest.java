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

import io.axual.ksml.client.testutil.PrefixResolver;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ResolvingDescribeAclsResultTest {
    private final PrefixResolver resolver = new PrefixResolver();

    @Test
    @DisplayName("ACL binding resource names are unresolved")
    void bindingNamesAreUnresolved() throws Exception {
        final KafkaFuture<Collection<AclBinding>> future = KafkaFuture.completedFuture(List.of(
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, "tenant-orders", PatternType.LITERAL),
                        new AccessControlEntry("User:alice", "*", AclOperation.READ, AclPermissionType.ALLOW))));

        final var result = new ResolvingDescribeAclsResult(future, resolver, resolver);

        assertThat(result.values().get()).extracting(b -> b.pattern().name()).containsExactly("orders");
    }

    @Test
    @DisplayName("A failed lookup propagates the failure")
    @SuppressWarnings("unchecked")
    void failurePropagates() {
        final KafkaFuture<Collection<AclBinding>> future = mock(KafkaFuture.class);
        final RuntimeException cause = new RuntimeException("boom");
        when(future.whenComplete(any())).thenAnswer(inv -> {
            final KafkaFuture.BiConsumer<Collection<AclBinding>, Throwable> action = inv.getArgument(0);
            action.accept(null, cause);
            return mock(KafkaFuture.class);
        });

        final var result = new ResolvingDescribeAclsResult(future, resolver, resolver);

        assertThatThrownBy(() -> result.values().get()).hasRootCauseMessage("boom");
    }
}
