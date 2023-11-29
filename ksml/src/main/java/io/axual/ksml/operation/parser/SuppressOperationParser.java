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


import io.axual.ksml.exception.KSMLParseException;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.operation.SuppressOperation;
import io.axual.ksml.parser.YamlNode;
import org.apache.kafka.streams.kstream.Suppressed;

import static io.axual.ksml.dsl.KSMLDSL.*;

public class SuppressOperationParser extends OperationParser<SuppressOperation> {
    public SuppressOperationParser(String name, TopologyResources resources) {
        super(name, resources);
    }

    @Override
    public SuppressOperation parse(YamlNode node) {
        if (node == null) return null;
        String suppressedType = parseString(node, SUPPRESS_UNTIL_ATTRIBUTE);
        if (suppressedType != null) {
            switch (suppressedType) {
                case SUPPRESS_UNTILTIMELIMIT:
                    return parseSuppressUntilTimeLimit(node);
                case SUPPRESS_UNTILWINDOWCLOSES:
                    return parseSuppressUntilWindowClose(node);
                default:
                    throw new KSMLParseException(node, "Unknown Suppressed dataType for suppress operation: " + suppressedType);
            }
        }
        throw new KSMLParseException(node, "Mandatory 'until' attribute is missing for Suppress operation");
    }

    private SuppressOperation parseSuppressUntilTimeLimit(YamlNode node) {
        var duration = parseDuration(node, SUPPRESS_DURATION_ATTRIBUTE);
        var bufferConfig = parseBufferConfig(node);
        return SuppressOperation.create(operationConfig(node), Suppressed.untilTimeLimit(duration, bufferConfig));
    }

    private SuppressOperation parseSuppressUntilWindowClose(YamlNode node) {
        var bufferConfig = parseStrictBufferConfig(node);
        return SuppressOperation.createWindowed(operationConfig(node), Suppressed.untilWindowCloses(bufferConfig));
    }

    private Suppressed.EagerBufferConfig parseBufferConfig(YamlNode node) {
        Suppressed.EagerBufferConfig result = null;

        // Check for a maxBytes setting
        String maxBytes = parseString(node, SUPPRESS_BUFFER_MAXBYTES);
        if (maxBytes != null) {
            result = Suppressed.BufferConfig.maxBytes(Long.parseLong(maxBytes));
        }

        // Check for a maxRecords setting
        String maxRecords = parseString(node, SUPPRESS_BUFFER_MAXRECORDS);
        if (maxRecords != null) {
            if (result == null) {
                result = Suppressed.BufferConfig.maxRecords(Long.parseLong(maxRecords));
            } else {
                result = result.withMaxRecords(Long.parseLong(maxRecords));
            }
        }

        // Check for a bufferFull strategy
        String bufferFullStrategy = parseString(node, SUPPRESS_BUFFERFULLSTRATEGY);
        if (SUPPRESS_BUFFERFULLSTRATEGY_EMIT.equals(bufferFullStrategy)) {
            if (result == null) {
                throw new KSMLParseException(node, "Can not instantiate BufferConfig without maxBytes and/or maxRecords setting");
            }
            result = result.emitEarlyWhenFull();
        }

        return result;
    }

    private Suppressed.StrictBufferConfig parseStrictBufferConfig(YamlNode node) {
        Suppressed.EagerBufferConfig result = parseBufferConfig(node);

        // Assume the BufferFullStrategy is SHUT_DOWN from here on
        if (result == null) {
            return Suppressed.BufferConfig.unbounded();
        }

        return result.shutDownWhenFull();
    }
}
