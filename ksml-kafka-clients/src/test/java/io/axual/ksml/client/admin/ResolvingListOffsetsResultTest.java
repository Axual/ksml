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

import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class ResolvingListOffsetsResultTest {
    private static final TopicPartition RESOLVED_PARTITION = new TopicPartition("tenant-orders", 0);
    private static final TopicPartition UNRESOLVED_PARTITION = new TopicPartition("orders", 0);

    private final PrefixResolver resolver = new PrefixResolver();

    @Test
    @DisplayName("Offsets are reported under the unresolved partition and resolved on lookup")
    void offsetsAreUnresolvedAndResolved() throws Exception {
        final var info = new ListOffsetsResultInfo(10L, 20L, Optional.empty());
        final var delegate = new ListOffsetsResult(Map.of(RESOLVED_PARTITION, KafkaFuture.completedFuture(info)));

        final var result = new ResolvingListOffsetsResult(delegate, resolver);

        assertThat(result.all().get()).containsOnlyKeys(UNRESOLVED_PARTITION);
        assertThat(result.partitionResult(UNRESOLVED_PARTITION).get().offset()).isEqualTo(10L);
    }
}
