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
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.exception.ExecutionException;
import io.axual.ksml.type.UserType;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.processor.RecordContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.axual.ksml.user.UserTestSupport.functionReturning;
import static io.axual.ksml.user.UserTestSupport.tags;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

class UserTopicNameExtractorTest {

    private static final UserType STRING = new UserType(DataString.DATATYPE);

    private static RecordContext recordContext() {
        final var context = mock(RecordContext.class);
        lenient().when(context.topic()).thenReturn("input");
        lenient().when(context.headers()).thenReturn(new RecordHeaders());
        return context;
    }

    @Test
    @DisplayName("a string function result is returned as the extracted topic name")
    void returnsExtractedTopicName() {
        final var extractor = new UserTopicNameExtractor(functionReturning(STRING, 3, new DataString("outTopic")), tags());
        assertThat(extractor.extract("key", "value", recordContext())).isEqualTo("outTopic");
    }

    @Test
    @DisplayName("a non-string function result throws an execution exception mentioning string")
    void failsWhenResultIsNotAString() {
        final var extractor = new UserTopicNameExtractor(functionReturning(STRING, 3, new DataInteger(1)), tags());
        final var context = recordContext();
        assertThatThrownBy(() -> extractor.extract("key", "value", context))
                .isInstanceOf(ExecutionException.class)
                .hasMessageContaining("string");
    }
}
