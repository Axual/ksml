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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class TransformKeyValueProcessorTest {

    private static final String[] NO_STORES = new String[0];

    @Mock
    private ProcessorContext<Object, Object> context;

    @Test
    void forwardsTransformedKeyValue() {
        final var processor = new TransformKeyValueProcessor("map", (stores, rec) -> new KeyValue<>("k2", "v2"), NO_STORES);
        processor.init(context);

        processor.process(new Record<>("k1", "v1", 0L));

        final ArgumentCaptor<Record<Object, Object>> captor = ArgumentCaptor.captor();
        verify(context).forward(captor.capture());
        assertThat(captor.getValue().key()).isEqualTo("k2");
        assertThat(captor.getValue().value()).isEqualTo("v2");
    }

    @Test
    void doesNotForwardWhenResultIsNull() {
        final var processor = new TransformKeyValueProcessor("map", (stores, rec) -> null, NO_STORES);
        processor.init(context);

        processor.process(new Record<>("k1", "v1", 0L));

        verify(context, never()).forward(org.mockito.ArgumentMatchers.any());
    }
}
