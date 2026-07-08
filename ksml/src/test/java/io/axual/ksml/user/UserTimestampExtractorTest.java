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

import io.axual.ksml.data.object.DataLong;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.exception.ExecutionException;
import io.axual.ksml.type.UserType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import static io.axual.ksml.user.UserTestSupport.functionReturning;
import static io.axual.ksml.user.UserTestSupport.tags;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class UserTimestampExtractorTest {

    private static final UserType LONG = new UserType(DataLong.DATATYPE);

    private static ConsumerRecord<Object, Object> consumerRecord() {
        return new ConsumerRecord<>("topic", 0, 0L, "key", "value");
    }

    @Test
    void returnsExtractedTimestamp() {
        final var extractor = new UserTimestampExtractor(functionReturning(LONG, 2, new DataLong(999L)), tags());
        assertThat(extractor.extract(consumerRecord(), 0L)).isEqualTo(999L);
    }

    @Test
    void failsWhenResultIsNotALong() {
        final var extractor = new UserTimestampExtractor(functionReturning(LONG, 2, new DataString("nope")), tags());
        final var rec = consumerRecord();
        assertThatThrownBy(() -> extractor.extract(rec, 0L))
                .isInstanceOf(ExecutionException.class)
                .hasMessageContaining("long");
    }
}
