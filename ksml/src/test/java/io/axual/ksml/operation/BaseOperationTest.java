package io.axual.ksml.operation;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2024 Axual B.V.
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

import io.axual.ksml.data.schema.LogicalSchema;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.schema.logical.DecimalLogicalType;
import io.axual.ksml.data.type.StructType;
import io.axual.ksml.exception.TopologyException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.kstream.TableJoined;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.axual.ksml.operation.OperationTestSupport.storeConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Covers {@link BaseOperation} behaviour shared by all operations: name validation, the
 * {@code toString()} rendering, and the wording of the type-check error.
 */
class BaseOperationTest {

    @Test
    @DisplayName("a valid operation name appears in the toString() output")
    void acceptsValidNameInToString() {
        final var operation = new CountOperation(storeConfig("validName"));
        assertThat(operation).asString().contains("validName");
    }

    @Test
    @DisplayName("an invalid operation name raises IllegalArgumentException")
    void rejectsInvalidName() {
        // An invalid name fails Kafka's Named validation, after which BaseOperation nulls the name,
        // appending a null metric-tag value then fails, surfacing the invalid configuration.
        final var config = storeConfig("invalid name!");
        assertThatThrownBy(() -> new CountOperation(config))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("a type-check error names the field and the reason, not only the record")
    void typeCheckErrorCarriesTheFullReason() {
        // Two records that differ only in the scale of a nested decimal field. Both are called
        // "Reading", so the top line of the error cannot tell the user anything useful on its own.
        final var pipelineType = structWithDecimal(10, 2);
        final var topicType = structWithDecimal(10, 3);

        final var operation = new TypeCheckingOperation();
        assertThatThrownBy(() -> operation.check(pipelineType, topicType))
                .isInstanceOf(TopologyException.class)
                .hasMessageContaining("amount")
                .hasMessageContaining("decimal(10,3) is not assignable from decimal(10,2)");
    }

    @Test
    @DisplayName("the partitioner of a table join passes the record headers to the user partitioner")
    @SuppressWarnings("unchecked")
    void tableJoinedPartitionerPassesHeaders() {
        final StreamPartitioner<Object, Object> partitioner = mock(StreamPartitioner.class);
        final var headers = new RecordHeaders();
        when(partitioner.partitions("topic", "key", null, headers, 4)).thenReturn(Optional.of(Set.of(1)));

        final var tableJoined = new TypeCheckingOperation().tableJoinedOf(partitioner, null);

        assertThat(new TableJoinedView(tableJoined).partitioner().partitions("topic", "key", null, headers, 4))
                .contains(Set.of(1));
    }

    @Test
    @DisplayName("the old method of the table-join partitioner passes empty headers to the user partitioner")
    @SuppressWarnings({"unchecked", "removal"})
    void tableJoinedPartitionerOldMethodPassesEmptyHeaders() {
        final StreamPartitioner<Object, Object> partitioner = mock(StreamPartitioner.class);
        when(partitioner.partitions(eq("topic"), eq("key"), isNull(), any(Headers.class), eq(4))).thenReturn(Optional.of(Set.of(1)));

        final var tableJoined = new TypeCheckingOperation().tableJoinedOf(partitioner, null);

        assertThat(new TableJoinedView(tableJoined).partitioner().partitions("topic", "key", null, 4))
                .contains(Set.of(1));
        verify(partitioner).partitions(eq("topic"), eq("key"), isNull(), argThat(h -> !h.iterator().hasNext()), eq(4));
    }

    private static StructType structWithDecimal(int precision, int scale) {
        final var field = new StructSchema.Field("amount", new LogicalSchema(new DecimalLogicalType(precision, scale)), null, 0);
        return new StructType(new StructSchema("io.ksml.example", "Reading", null, List.of(field)));
    }

    /** Minimal operation that exposes the protected type check so the error wording can be asserted. */
    private static class TypeCheckingOperation extends BaseOperation {
        TypeCheckingOperation() {
            super(storeConfig("typeCheck"));
        }

        void check(StructType pipelineType, StructType topicType) {
            checkType("Target topic valueType", topicType, superOf(pipelineType));
        }
    }

    /** TableJoined has no public getters; a subclass can read its protected fields. */
    private static class TableJoinedView extends TableJoined<Object, Object> {
        TableJoinedView(TableJoined<Object, Object> tableJoined) {
            super(tableJoined);
        }

        StreamPartitioner<Object, Void> partitioner() {
            return partitioner;
        }
    }
}
