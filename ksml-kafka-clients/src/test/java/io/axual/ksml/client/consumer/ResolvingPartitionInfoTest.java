package io.axual.ksml.client.consumer;

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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ResolvingPartitionInfoTest {
    private final ResolvingPartitionInfo partitionInfo = new ResolvingPartitionInfo("orders", 3);

    @Test
    @DisplayName("The topic and partition are exposed as provided")
    void topicAndPartitionAreExposed() {
        assertThat(partitionInfo.topic()).isEqualTo("orders");
        assertThat(partitionInfo.partition()).isEqualTo(3);
    }

    @Test
    @DisplayName("Leader information is not supported")
    void leaderIsNotSupported() {
        assertThatThrownBy(partitionInfo::leader)
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("leader");
    }

    @Test
    @DisplayName("Replica information is not supported")
    void replicasAreNotSupported() {
        assertThatThrownBy(partitionInfo::replicas)
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("replicas");
        assertThatThrownBy(partitionInfo::inSyncReplicas)
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("inSyncReplicas");
    }

    @Test
    @DisplayName("toString reports the topic, partition and the unsupported fields")
    void toStringReportsUnsupportedFields() {
        assertThat(partitionInfo).asString()
                .contains("orders")
                .contains("partition = 3")
                .contains("Not Supported");
    }
}
