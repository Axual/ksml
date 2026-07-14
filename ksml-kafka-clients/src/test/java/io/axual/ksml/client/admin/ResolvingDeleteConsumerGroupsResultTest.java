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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ResolvingDeleteConsumerGroupsResultTest {
    private final PrefixResolver resolver = new PrefixResolver();

    @Test
    @DisplayName("Deleted groups are reported under their unresolved id, dropping unresolvable ones")
    void deletedGroupsAreUnresolved() {
        final Map<String, KafkaFuture<Void>> futures = new HashMap<>();
        futures.put("tenant-group", KafkaFuture.completedFuture(null));
        futures.put("unprefixed", KafkaFuture.completedFuture(null));

        final var result = new ResolvingDeleteConsumerGroupsResult(futures, resolver);

        assertThat(result.deletedGroups()).containsOnlyKeys("group");
    }
}
