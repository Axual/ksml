package io.axual.ksml.operation.processor;

/*-
 * ========================LICENSE_START=================================
 * KSML
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

import io.axual.ksml.proxy.store.TimestampedKeyValueStoreProxy;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OperationProcessorTest {
    private static final String STORE = "store";

    @Mock
    private ProcessorContext<Object, Object> context;
    @Mock
    private TimestampedKeyValueStore<Object, Object> store;

    private TestProcessor processor;

    /** Writes to its store without a timestamp while processing a record. */
    private static class TestProcessor extends OperationProcessor {
        TestProcessor() {
            super("test", new String[]{STORE});
        }

        TimestampedKeyValueStoreProxy proxy() {
            return (TimestampedKeyValueStoreProxy) stores.get(STORE);
        }

        @Override
        protected void processRecord(Record<Object, Object> rec) {
            proxy().put("key", "value");
        }

        void punctuate(long timestamp) {
            runAt(timestamp, () -> proxy().put("key", "value"));
        }
    }

    @BeforeEach
    void setUp() {
        when(context.getStateStore(STORE)).thenReturn(store);
        processor = new TestProcessor();
        processor.init(context);
    }

    private long writtenTimestamp() {
        final ArgumentCaptor<ValueAndTimestamp<Object>> captor = ArgumentCaptor.captor();
        verify(store).put(any(), captor.capture());
        return captor.getValue().timestamp();
    }

    @Test
    @DisplayName("a write without a timestamp uses the timestamp of the record being processed")
    void writeDuringProcessUsesRecordTimestamp() {
        processor.process(new Record<>("k", "v", 1234L));
        assertThat(writtenTimestamp()).isEqualTo(1234L);
    }

    @Test
    @DisplayName("a write without a timestamp inside runAt uses the given timestamp")
    void writeInsideRunAtUsesGivenTimestamp() {
        processor.punctuate(5678L);
        assertThat(writtenTimestamp()).isEqualTo(5678L);
    }

    @Test
    @DisplayName("a write without a timestamp outside a record or runAt fails")
    void writeOutsideFails() {
        processor.punctuate(1L);
        final var proxy = processor.proxy();
        assertThatThrownBy(() -> proxy.put("key", "value"))
                .hasMessageContaining("No record is being processed");
    }
}
