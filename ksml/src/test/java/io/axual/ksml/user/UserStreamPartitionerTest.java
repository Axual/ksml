package io.axual.ksml.user;

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

import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataList;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.definition.ParameterDefinition;
import io.axual.ksml.definition.PythonSource;
import io.axual.ksml.definition.StreamPartitionerDefinition;
import io.axual.ksml.type.UserType;
import io.axual.ksml.user.UserTestSupport.RecordingFunction;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;

import static io.axual.ksml.user.UserTestSupport.functionReturning;
import static io.axual.ksml.user.UserTestSupport.params;
import static io.axual.ksml.user.UserTestSupport.tags;
import static org.assertj.core.api.Assertions.assertThat;

class UserStreamPartitionerTest {

    private static final UserType PARTITIONS = new UserType(UserStreamPartitioner.EXPECTED_RESULT_TYPE);

    private UserStreamPartitioner partitioner(DataObject result) {
        return new UserStreamPartitioner(functionReturning(PARTITIONS, 4, result), tags());
    }

    @Test
    @DisplayName("an integer result maps to a set with that single partition")
    void returnsSinglePartition() {
        assertThat(partitioner(new DataInteger(2)).partitions("topic", "key", "value", new RecordHeaders(), 8))
                .contains(Set.of(2));
    }

    @Test
    @DisplayName("a list result maps to a set containing all listed partitions")
    void returnsSetOfPartitions() {
        final var list = new DataList(DataInteger.DATATYPE);
        list.add(new DataInteger(1));
        list.add(new DataInteger(3));

        assertThat(partitioner(list).partitions("topic", "key", "value", new RecordHeaders(), 8))
                .contains(Set.of(1, 3));
    }

    @Test
    @DisplayName("a result that is neither integer nor list yields no partitions")
    void returnsEmptyWhenResultIsNeitherIntegerNorList() {
        assertThat(partitioner(new DataString("nonsense")).partitions("topic", "key", "value", new RecordHeaders(), 8))
                .isEmpty();
    }

    @Test
    @DisplayName("the old method without headers gives the same result as the new one")
    @SuppressWarnings("removal")
    void oldMethodWithoutHeadersGivesSameResult() {
        assertThat(partitioner(new DataInteger(2)).partitions("topic", "key", "value", 8))
                .contains(Set.of(2));
    }

    @Test
    @DisplayName("a streamPartitioner function has an optional headers parameter after numPartitions")
    void definitionHasOptionalHeadersParameter() {
        final var params = streamPartitionerParameters();

        assertThat(params).extracting(ParameterDefinition::name)
                .containsExactly("topic", "key", "value", "numPartitions", "headers");
        assertThat(params[4].isOptional()).isTrue();
    }

    @Test
    @DisplayName("the function gets the record headers as a list of key and value")
    void passesHeadersToFunction() {
        final var function = new RecordingFunction(PARTITIONS, streamPartitionerParameters(), new DataInteger(0));
        final var headers = new RecordHeaders();
        headers.add("region", "eu".getBytes(StandardCharsets.UTF_8));

        new UserStreamPartitioner(function, tags()).partitions("topic", "key", "value", headers, 4);

        assertThat(function.lastArguments).hasSize(5);
        final var passedHeaders = (DataList) function.lastArguments[4];
        assertThat(passedHeaders.size()).isEqualTo(1);
        final var header = (DataStruct) passedHeaders.get(0);
        assertThat(header.get("key")).isEqualTo(new DataString("region"));
        assertThat(header.get("value")).isEqualTo(new DataString("eu"));
    }

    @Test
    @DisplayName("null headers reach the function as an empty list")
    void passesEmptyListForNullHeaders() {
        final var function = new RecordingFunction(PARTITIONS, streamPartitionerParameters(), new DataInteger(0));

        new UserStreamPartitioner(function, tags()).partitions("topic", "key", "value", null, 4);

        assertThat((DataList) function.lastArguments[4]).isEmpty();
    }

    @Test
    @DisplayName("a function without a headers parameter gets only the first four arguments")
    void doesNotPassHeadersToFunctionWithoutHeadersParameter() {
        final var function = new RecordingFunction(PARTITIONS, params(4), new DataInteger(0));
        final var headers = new RecordHeaders();
        headers.add("region", "eu".getBytes(StandardCharsets.UTF_8));

        assertThat(new UserStreamPartitioner(function, tags()).partitions("topic", "key", "value", headers, 4))
                .contains(Set.of(0));
        assertThat(function.lastArguments).hasSize(4);
    }

    private static ParameterDefinition[] streamPartitionerParameters() {
        final var function = FunctionDefinition.as("generic", "fn", List.of(),
                PythonSource.of((String) null, (String) null, (String) null), PARTITIONS, List.of());
        return new StreamPartitionerDefinition(function).parameters();
    }
}
