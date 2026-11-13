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

import io.axual.ksml.data.object.DataLong;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.exception.ExecutionException;
import io.axual.ksml.proxy.context.ProcessorContextProxy;
import io.axual.ksml.type.UserType;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PunctuateProcessorTest {
    private static final Duration INTERVAL = Duration.ofMinutes(1);
    private static final ProcessorContextProxy.Target NEXT = new ProcessorContextProxy.Target("flush-next", new UserType(DataString.DATATYPE), new UserType(DataLong.DATATYPE));

    @Mock
    private ProcessorContext<Object, Object> context;
    @Mock
    private Cancellable cancellable;

    private final List<ProcessorContextProxy> proxies = new ArrayList<>();

    private PunctuateProcessor processor(ProcessorContextProxy.Target next, PunctuateProcessor.PunctuateAction action) {
        return new PunctuateProcessor("flush", new PunctuateProcessor.Schedule(INTERVAL, PunctuationType.STREAM_TIME, next, Map.of()), action, new String[0]);
    }

    /** Initializes the processor and returns the punctuator it scheduled. */
    private Punctuator init(PunctuateProcessor processor) {
        final var captor = ArgumentCaptor.forClass(Punctuator.class);
        when(context.schedule(eq(INTERVAL), eq(PunctuationType.STREAM_TIME), captor.capture())).thenReturn(cancellable);
        processor.init(context);
        return captor.getValue();
    }

    @Test
    @DisplayName("init schedules the punctuator once, and close cancels it")
    void scheduleAndCancel() {
        final var processor = processor(NEXT, (stores, timestamp, proxy) -> {
        });
        init(processor);
        processor.close();
        verify(cancellable).cancel();
    }

    @Test
    @DisplayName("input records are forwarded unchanged, to the next operation only")
    void passThroughToNext() {
        final var processor = processor(NEXT, (stores, timestamp, proxy) -> {
        });
        init(processor);
        final var rec = new Record<Object, Object>("k", "v", 5L);
        processor.process(rec);
        verify(context).forward(rec, "flush-next");
    }

    @Test
    @DisplayName("without a named next operation, input records go to all children")
    void passThroughToAll() {
        final var processor = processor(new ProcessorContextProxy.Target(null, NEXT.keyType(), NEXT.valueType()), (stores, timestamp, proxy) -> {
        });
        init(processor);
        final var rec = new Record<Object, Object>("k", "v", 5L);
        processor.process(rec);
        verify(context).forward(rec);
    }

    @Test
    @DisplayName("each punctuation gets a new context with the punctuation timestamp, which is invalid after the call")
    void contextPerCall() {
        final var processor = processor(NEXT, (stores, timestamp, proxy) -> {
            proxy.forward("a", timestamp, null, null, null);
            proxies.add(proxy);
        });
        final var punctuator = init(processor);
        punctuator.punctuate(1000L);
        punctuator.punctuate(2000L);
        assertThat(proxies).hasSize(2).doesNotHaveDuplicates();
        @SuppressWarnings("unchecked") final ArgumentCaptor<Record<Object, Object>> captor = ArgumentCaptor.forClass(Record.class);
        verify(context, org.mockito.Mockito.times(2)).forward(captor.capture(), eq("flush-next"));
        assertThat(captor.getAllValues()).extracting(Record::timestamp).containsExactly(1000L, 2000L);
        assertThatThrownBy(proxies.get(0)::taskId).hasMessageContaining("no longer valid");
    }

    @Test
    @DisplayName("an error in the punctuator names the operation and the timestamp, and the context is still invalidated")
    void errorWrapped() {
        final var processor = processor(NEXT, (stores, timestamp, proxy) -> {
            proxies.add(proxy);
            throw new IllegalStateException("boom");
        });
        final var punctuator = init(processor);
        assertThatThrownBy(() -> punctuator.punctuate(1000L))
                .isInstanceOf(ExecutionException.class)
                .hasMessageContaining("Error in punctuate operation 'flush' during the punctuation at timestamp 1000, not while processing a record: boom")
                .hasCauseInstanceOf(IllegalStateException.class);
        assertThatThrownBy(proxies.get(0)::taskId).hasMessageContaining("no longer valid");
        verify(context, org.mockito.Mockito.never()).forward(any(Record.class), any(String.class));
    }
}
