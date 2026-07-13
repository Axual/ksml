package io.axual.ksml.operation.processor;

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

import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static io.axual.ksml.operation.processor.PeekProcessorTest.fixedKeyRecord;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class FilterNotProcessorTest {

    private static final String[] NO_STORES = new String[0];

    @Mock
    private FixedKeyProcessorContext<Object, Object> context;

    @Test
    @DisplayName("filterNot forwards the record when the predicate evaluates to false")
    void forwardsRecordWhenPredicateIsFalse() {
        final var processor = new FilterNotProcessor("filterNot", (stores, rec) -> false, NO_STORES);
        processor.init(context);

        final var rec = fixedKeyRecord("key", "value");
        processor.process(rec);

        verify(context).forward(rec);
    }

    @Test
    @DisplayName("filterNot drops the record when the predicate evaluates to true")
    void dropsRecordWhenPredicateIsTrue() {
        final var processor = new FilterNotProcessor("filterNot", (stores, rec) -> true, NO_STORES);
        processor.init(context);

        processor.process(fixedKeyRecord("key", "value"));

        verify(context, never()).forward(any());
    }
}
