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
import io.axual.ksml.execution.FatalError;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.operation.WindowByTimeOperation;
import io.axual.ksml.parser.YamlNode;
import org.apache.kafka.streams.kstream.SlidingWindows;

import static io.axual.ksml.dsl.KSMLDSL.*;

public class WindowByTimeOperationParser extends OperationParser<WindowByTimeOperation> {
    public WindowByTimeOperationParser(String prefix, String name, TopologyResources resources) {
        super(prefix, name, resources);
    }

    @Override
    public WindowByTimeOperation parse(YamlNode node) {
        if (node == null) return null;
        String windowType = parseString(node, TimeWindows.WINDOW_TYPE);
        if (windowType != null) {
            return switch (windowType) {
                case TimeWindows.TYPE_TUMBLING -> parseTumblingWindow(node);
                case TimeWindows.TYPE_HOPPING -> parseHoppingWindow(node);
                case TimeWindows.TYPE_SLIDING -> parseSlidingWindow(node);
                default ->
                        throw new KSMLParseException(node, "Unknown WindowType for windowByTime operation: " + windowType + " (choose tumbling, hopping or sliding)");
            };
        }
        throw new KSMLParseException(node, "WindowType missing for windowedBy operation, choose tumbling, hopping or sliding");
    }

    private WindowByTimeOperation parseTumblingWindow(YamlNode node) {
        final var duration = parseDuration(node, TimeWindows.DURATION, "Missing duration attribute for tumbling window");
        final var grace = parseDuration(node, TimeWindows.GRACE);
        final var timeWindows = (grace != null && grace.toMillis() > 0)
                ? org.apache.kafka.streams.kstream.TimeWindows.ofSizeAndGrace(duration, grace)
                : org.apache.kafka.streams.kstream.TimeWindows.ofSizeWithNoGrace(duration);
        return new WindowByTimeOperation(operationConfig(node), timeWindows);
    }

    private WindowByTimeOperation parseHoppingWindow(YamlNode node) {
        final var duration = parseDuration(node, TimeWindows.DURATION, "Missing duration attribute for hopping window");
        final var advanceBy = parseDuration(node, TimeWindows.ADVANCE_BY, "Missing advanceBy attribute for hopping window");
        final var grace = parseDuration(node, TimeWindows.GRACE);
        if (advanceBy.toMillis() > duration.toMillis()) {
            throw FatalError.parseError(node, "A hopping window can not advanceBy more than its duration");
        }

        final var timeWindows = (grace != null && grace.toMillis() > 0)
                ? org.apache.kafka.streams.kstream.TimeWindows.ofSizeAndGrace(duration, grace).advanceBy(advanceBy)
                : org.apache.kafka.streams.kstream.TimeWindows.ofSizeWithNoGrace(duration).advanceBy(advanceBy);
        return new WindowByTimeOperation(operationConfig(node), timeWindows);
    }

    private WindowByTimeOperation parseSlidingWindow(YamlNode node) {
        final var timeDifference = parseDuration(node, TimeWindows.TIME_DIFFERENCE, "Missing timeDifference attribute for sliding window");
        final var grace = parseDuration(node, TimeWindows.GRACE);
        final var slidingWindows = (grace != null && grace.toMillis() > 0)
                ? SlidingWindows.ofTimeDifferenceAndGrace(timeDifference, grace)
                : SlidingWindows.ofTimeDifferenceWithNoGrace(timeDifference);
        return new WindowByTimeOperation(operationConfig(node), slidingWindows);
    }
}
