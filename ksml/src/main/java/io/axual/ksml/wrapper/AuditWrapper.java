package io.axual.ksml.wrapper;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2025 Axual B.V.
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

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.*;

@Slf4j
public class AuditWrapper implements ProcessorWrapper {
    @Override
    public <KIn, VIn, KOut, VOut> WrappedProcessorSupplier<KIn, VIn, KOut, VOut> wrapProcessorSupplier(String processorName, ProcessorSupplier<KIn, VIn, KOut, VOut> processorSupplier) {
        log.info("Wrapping processor {} just for fun", processorName);
        return () -> {
            log.info("Performing GET method on processor {}", processorName);
            final var processor = processorSupplier.get();
            return new Processor<>() {
                @Override
                public void init(final ProcessorContext<KOut, VOut> context) {
                    log.info("Processor {} init is called", processorName);
                    processor.init(context);
                }

                @Override
                public void process(Record<KIn, VIn> rec) {
                    log.info("Processor {} process is called", processorName);
                    processor.process(rec);
                }
            };
        };
    }

    @Override
    public <KIn, VIn, VOut> WrappedFixedKeyProcessorSupplier<KIn, VIn, VOut> wrapFixedKeyProcessorSupplier(String processorName, FixedKeyProcessorSupplier<KIn, VIn, VOut> processorSupplier) {
        log.info("Wrapping fixed key processor {} just for fun", processorName);
        return () -> {
            log.info("Performing GET method on fixed key processor {}", processorName);
            return processorSupplier.get();
        };
    }
}
