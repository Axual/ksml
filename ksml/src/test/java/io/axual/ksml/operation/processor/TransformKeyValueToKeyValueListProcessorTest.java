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

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class TransformKeyValueToKeyValueListProcessorTest {

    private static final String[] NO_STORES = new String[0];

    @Mock
    private ProcessorContext<Object, Object> context;

    @Test
    @DisplayName("forwards a separate record for each key-value pair returned by the transform")
    void forwardsEachKeyValueInList() {
        final List<KeyValue<Object, Object>> list = List.of(new KeyValue<>("k1", "v1"), new KeyValue<>("k2", "v2"));
        final var processor = new TransformKeyValueToKeyValueListProcessor("flatMap", (stores, rec) -> list, NO_STORES);
        processor.init(context);

        processor.process(new Record<>("key", "value", 0L));

        verify(context, times(2)).forward(any());
    }

    @Test
    @DisplayName("does not forward anything when the transform returns null")
    void doesNotForwardWhenResultIsNull() {
        final var processor = new TransformKeyValueToKeyValueListProcessor("flatMap", (stores, rec) -> null, NO_STORES);
        processor.init(context);

        processor.process(new Record<>("key", "value", 0L));

        verify(context, never()).forward(any());
    }
}
