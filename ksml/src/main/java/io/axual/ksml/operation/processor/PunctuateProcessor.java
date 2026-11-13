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

import io.axual.ksml.exception.ExecutionException;
import io.axual.ksml.proxy.context.ProcessorContextProxy;
import io.axual.ksml.store.StateStores;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

import java.time.Duration;
import java.util.Map;

/**
 * Passes records through unchanged, and calls a punctuator on a schedule. The punctuator forwards records through a
 * {@link ProcessorContextProxy}, to the next operation or to a named operation further down the pipeline.
 */
public class PunctuateProcessor extends OperationProcessor {
    public interface PunctuateAction {
        void apply(StateStores stores, long timestamp, ProcessorContextProxy context);
    }

    /**
     * The schedule and the forward targets of a punctuate operation.
     *
     * @param interval     the time between two punctuations
     * @param type         the time that drives the schedule
     * @param next         the next operation, which gets the input records and records forwarded without a name
     * @param namedTargets the named operations further down, filled in while the rest of the pipeline is built
     */
    public record Schedule(Duration interval, PunctuationType type, ProcessorContextProxy.Target next,
                           Map<String, ProcessorContextProxy.Target> namedTargets) {
    }

    private final Schedule schedule;
    private final PunctuateAction action;
    private Cancellable cancellable;

    public PunctuateProcessor(String name, Schedule schedule, PunctuateAction action, String[] storeNames) {
        super(name, storeNames);
        this.schedule = schedule;
        this.action = action;
    }

    @Override
    public void init(ProcessorContext<Object, Object> context) {
        super.init(context);
        cancellable = context.schedule(schedule.interval(), schedule.type(), this::punctuate);
    }

    @Override
    protected void processRecord(Record<Object, Object> rec) {
        final var childName = schedule.next().childName();
        if (childName != null) context.forward(rec, childName);
        else context.forward(rec);
    }

    private void punctuate(long timestamp) {
        runAt(timestamp, () -> {
            final var proxy = new ProcessorContextProxy(context, timestamp, schedule.next(), schedule.namedTargets());
            try {
                action.apply(stores, timestamp, proxy);
            } catch (RuntimeException e) {
                throw new ExecutionException("Error in punctuate operation '" + name() + "' during the punctuation at timestamp " + timestamp + ", not while processing a record: " + e.getMessage(), e);
            } finally {
                proxy.invalidate();
            }
        });
    }

    @Override
    public void close() {
        if (cancellable != null) cancellable.cancel();
        cancellable = null;
    }
}
