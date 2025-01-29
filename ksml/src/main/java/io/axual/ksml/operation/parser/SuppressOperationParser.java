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


import io.axual.ksml.data.schema.EnumSchema;
import io.axual.ksml.data.type.Symbol;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.exception.TopologyException;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.operation.SuppressOperation;
import io.axual.ksml.parser.ChoiceParser;
import io.axual.ksml.parser.StructsParser;
import org.apache.kafka.streams.kstream.Suppressed;

import java.util.List;
import java.util.Map;

import static io.axual.ksml.dsl.KSMLDSL.Operations;

public class SuppressOperationParser extends OperationParser<SuppressOperation> {
    private final StructsParser<SuppressOperation> untilTimeLimitParser;
    private final StructsParser<SuppressOperation> untilWindowClosesParser;

    public SuppressOperationParser(TopologyResources resources) {
        super(KSMLDSL.Operations.SUPPRESS, resources);
        final var bufferFullSchema = new EnumSchema(
                SCHEMA_NAMESPACE,
                "BufferFullStrategy",
                "What to do when the buffer is full",
                List.of(new Symbol(Operations.Suppress.BUFFER_FULL_STRATEGY_EMIT), new Symbol(Operations.Suppress.BUFFER_FULL_STRATEGY_SHUTDOWN)));

        untilTimeLimitParser = structsParser(
                SuppressOperation.class,
                "UntilTimeLimit",
                "Operation to suppress messages in the source stream until a time limit is reached",
                operationNameField(),
                durationField(Operations.Suppress.DURATION, "The duration for which messages are suppressed"),
                optional(stringField(Operations.Suppress.BUFFER_MAXBYTES, "The maximum number of bytes in the buffer")),
                optional(stringField(Operations.Suppress.BUFFER_MAXRECORDS, "The maximum number of records in the buffer")),
                optional(enumField(Operations.Suppress.BUFFER_FULL_STRATEGY, bufferFullSchema)),
                (name, duration, maxBytes, maxRecords, strategy, tags) -> {
                    final var bufferConfig = bufferConfig(maxBytes, maxRecords, strategy);
                    return SuppressOperation.create(
                            operationConfig(name, tags),
                            Suppressed.untilTimeLimit(duration, bufferConfig));
                });
        untilWindowClosesParser = structsParser(
                SuppressOperation.class,
                "UntilWindowCloses",
                "Operation to suppress messages in the source stream until a window limit is reached",
                operationNameField(),
                optional(stringField(Operations.Suppress.BUFFER_MAXBYTES, "The maximum number of bytes in the buffer")),
                optional(stringField(Operations.Suppress.BUFFER_MAXRECORDS, "The maximum number of records in the buffer")),
                optional(enumField(Operations.Suppress.BUFFER_FULL_STRATEGY, bufferFullSchema)),
                (name, maxBytes, maxRecords, strategy, tags) -> {
                    final var bufferConfig = strictBufferConfig(bufferConfig(maxBytes, maxRecords, strategy));
                    return SuppressOperation.createWindowed(
                            operationConfig(name, tags),
                            Suppressed.untilWindowCloses(bufferConfig));
                });
    }

    public StructsParser<SuppressOperation> parser() {
        return new ChoiceParser<>(
                Operations.Suppress.UNTIL,
                "SuppressType",
                "Operation to suppress messages in the source stream until a certain limit is reached",
                null,
                Map.of(Operations.Suppress.UNTIL_TIME_LIMIT, untilTimeLimitParser,
                        Operations.Suppress.UNTIL_WINDOW_CLOSES, untilWindowClosesParser));
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
