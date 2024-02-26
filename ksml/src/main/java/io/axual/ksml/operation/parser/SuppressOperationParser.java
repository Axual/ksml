package io.axual.ksml.operation.parser;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2023 Axual B.V.
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


import io.axual.ksml.exception.TopologyException;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.operation.SuppressOperation;
import io.axual.ksml.parser.StructParser;
import org.apache.kafka.streams.kstream.Suppressed;

import static io.axual.ksml.dsl.KSMLDSL.Operations;

public class SuppressOperationParser extends OperationParser<SuppressOperation> {
    public SuppressOperationParser(TopologyResources resources) {
        super("suppress", resources);
    }

    public StructParser<SuppressOperation> parser() {
        return structParser(
                SuppressOperation.class,
                "Operation to suppress messages in the source stream until a certain limit is reached",
                operationTypeField(Operations.SUPPRESS),
                nameField(),
                stringField(Operations.Suppress.UNTIL, true, "The method by which messages are held, either \"" + Operations.Suppress.UNTIL_TIME_LIMIT + "\", or \"" + Operations.Suppress.UNTIL_WINDOW_CLOSES + "\""),
                durationField(Operations.Suppress.DURATION, true, "The duration for which messages are suppressed"),
                stringField(Operations.Suppress.BUFFER_MAXBYTES, false, "The maximum number of bytes in the buffer"),
                stringField(Operations.Suppress.BUFFER_MAXRECORDS, false, "The maximum number of records in the buffer"),
                stringField(Operations.Suppress.BUFFER_FULL_STRATEGY, false, "What to do when the buffer is full, either \"" + Operations.Suppress.BUFFER_FULL_STRATEGY_EMIT + "\", or \"" + Operations.Suppress.BUFFER_FULL_STRATEGY_SHUTDOWN + "\""),
                (type, name, until, duration, maxBytes, maxRecords, strategy) -> {
                    switch (until) {
                        case Operations.Suppress.UNTIL_TIME_LIMIT -> {
                            final var bufferConfig = bufferConfig(maxBytes, maxRecords, strategy);
                            return SuppressOperation.create(
                                    operationConfig(name),
                                    Suppressed.untilTimeLimit(duration, bufferConfig));
                        }
                        case Operations.Suppress.UNTIL_WINDOW_CLOSES -> {
                            final var bufferConfig = strictBufferConfig(bufferConfig(maxBytes, maxRecords, strategy));
                            return SuppressOperation.createWindowed(
                                    operationConfig(name),
                                    Suppressed.untilWindowCloses(bufferConfig));
                        }
                    }
                    throw new TopologyException("Unknown Until type for suppress operation: " + until);
                });
    }

    private Suppressed.EagerBufferConfig bufferConfig(String maxBytes, String maxRecords, String bufferFullStrategy) {
        Suppressed.EagerBufferConfig result = null;

        // Check for a maxBytes setting
        if (maxBytes != null) {
            result = Suppressed.BufferConfig.maxBytes(Long.parseLong(maxBytes));
        }

        // Check for a maxRecords setting
        if (maxRecords != null) {
            if (result == null) {
                result = Suppressed.BufferConfig.maxRecords(Long.parseLong(maxRecords));
            } else {
                result = result.withMaxRecords(Long.parseLong(maxRecords));
            }
        }

        // Check for a bufferFull strategy
        if (Operations.Suppress.BUFFER_FULL_STRATEGY_EMIT.equals(bufferFullStrategy)) {
            if (result == null) {
                throw new TopologyException("Can not instantiate BufferConfig without maxBytes and/or maxRecords setting");
            }
            result = result.emitEarlyWhenFull();
        }

        return result;
    }

    private Suppressed.StrictBufferConfig strictBufferConfig(Suppressed.EagerBufferConfig config) {
        // Assume the BufferFullStrategy is SHUT_DOWN from here on
        if (config == null) {
            return Suppressed.BufferConfig.unbounded();
        }

        return config.shutDownWhenFull();
    }
}
