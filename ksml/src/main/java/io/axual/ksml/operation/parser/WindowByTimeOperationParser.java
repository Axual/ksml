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


import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.exception.TopologyException;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.operation.WindowByTimeOperation;
import io.axual.ksml.parser.ChoiceParser;
import io.axual.ksml.parser.StructsParser;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.TimeWindows;

import java.util.Map;

public class WindowByTimeOperationParser extends OperationParser<WindowByTimeOperation> {
    private static final String DOC = "Operation to window records based on time criteria";
    private final StructsParser<WindowByTimeOperation> tumblingParser;
    private final StructsParser<WindowByTimeOperation> hoppingParser;
    private final StructsParser<WindowByTimeOperation> slidingParser;

    public WindowByTimeOperationParser(TopologyResources resources) {
        super(KSMLDSL.Operations.WINDOW_BY_TIME, resources);

        final var durationParser = durationField(KSMLDSL.TimeWindows.DURATION, "The duration of time windows");
        final var advanceByParser = durationField(KSMLDSL.TimeWindows.ADVANCE_BY, "The amount of time to increase time windows by");
        final var graceParser = durationField(KSMLDSL.TimeWindows.GRACE, "The grace period, during which out-of-order records can still be processed");
        final var timeDifferenceParser = durationField(KSMLDSL.TimeWindows.TIME_DIFFERENCE, "The maximum amount of time difference between two records");

        tumblingParser = structsParser(
                WindowByTimeOperation.class,
                "WithTumblingWindow",
                DOC,
                operationNameField(),
                durationParser,
                optional(graceParser),
                (name, duration, grace, tags) -> {
                    final var timeWindows = (grace != null && grace.toMillis() > 0)
                            ? TimeWindows.ofSizeAndGrace(duration, grace)
                            : TimeWindows.ofSizeWithNoGrace(duration);
                    return new WindowByTimeOperation(operationConfig(name, tags), timeWindows);
                });

        hoppingParser = structsParser(
                WindowByTimeOperation.class,
                "WithHoppingWindow",
                DOC,
                operationNameField(),
                durationParser,
                advanceByParser,
                optional(graceParser),
                (name, duration, advanceBy, grace, tags) -> {
                    if (advanceBy.toMillis() > duration.toMillis()) {
                        throw new TopologyException("A hopping window can not advanceBy more than its duration");
                    }
                    final var timeWindows = (grace != null && grace.toMillis() > 0)
                            ? org.apache.kafka.streams.kstream.TimeWindows.ofSizeAndGrace(duration, grace).advanceBy(advanceBy)
                            : org.apache.kafka.streams.kstream.TimeWindows.ofSizeWithNoGrace(duration).advanceBy(advanceBy);
                    return new WindowByTimeOperation(operationConfig(name, tags), timeWindows);
                });

        slidingParser = structsParser(
                WindowByTimeOperation.class,
                "WithSlidingWindow",
                DOC,
                operationNameField(),
                timeDifferenceParser,
                optional(graceParser),
                (name, timeDifference, grace, tags) -> {
                    final var slidingWindows = (grace != null && grace.toMillis() > 0)
                            ? SlidingWindows.ofTimeDifferenceAndGrace(timeDifference, grace)
                            : SlidingWindows.ofTimeDifferenceWithNoGrace(timeDifference);
                    return new WindowByTimeOperation(operationConfig(name, tags), slidingWindows);
                });
    }

    @Override
    public StructsParser<WindowByTimeOperation> parser() {
        return new ChoiceParser<>(
                KSMLDSL.TimeWindows.WINDOW_TYPE,
                "WindowType",
                "time window",
                null,
                Map.of(KSMLDSL.TimeWindows.TYPE_TUMBLING, tumblingParser,
                        KSMLDSL.TimeWindows.TYPE_HOPPING, hoppingParser,
                        KSMLDSL.TimeWindows.TYPE_SLIDING, slidingParser));
    }
}
