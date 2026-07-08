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
import io.axual.ksml.type.UserType;
import org.junit.jupiter.api.Test;

import static io.axual.ksml.user.UserTestSupport.functionReturning;
import static io.axual.ksml.user.UserTestSupport.tags;
import static org.assertj.core.api.Assertions.assertThat;

class UserStreamPartitionerTest {

    private static final UserType PARTITIONS = new UserType(UserStreamPartitioner.EXPECTED_RESULT_TYPE);

    private UserStreamPartitioner partitioner(DataObject result) {
        return new UserStreamPartitioner(functionReturning(PARTITIONS, 4, result), tags());
    }

    @Test
    void returnsSinglePartition() {
        assertThat(partitioner(new DataInteger(2)).partitions("topic", "key", "value", 8))
                .contains(java.util.Set.of(2));
    }

    @Test
    void returnsSetOfPartitions() {
        final var list = new DataList(DataInteger.DATATYPE);
        list.add(new DataInteger(1));
        list.add(new DataInteger(3));

        assertThat(partitioner(list).partitions("topic", "key", "value", 8))
                .contains(java.util.Set.of(1, 3));
    }

    @Test
    void returnsEmptyWhenResultIsNeitherIntegerNorList() {
        assertThat(partitioner(new DataString("nonsense")).partitions("topic", "key", "value", 8))
                .isEmpty();
    }
}
