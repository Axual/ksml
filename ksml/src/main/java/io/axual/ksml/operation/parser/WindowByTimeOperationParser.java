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
import io.axual.ksml.parser.StructsParser;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.TimeWindows;

public class WindowByTimeOperationParser extends OperationParser<WindowByTimeOperation> {
    public WindowByTimeOperationParser(TopologyResources resources) {
        super(KSMLDSL.Operations.WINDOW_BY_TIME, resources);
    }

    @Override
    public StructsParser<WindowByTimeOperation> parser() {
        return structsParser(
                WindowByTimeOperation.class,
                "",
                "Operation to reduce a series of records into a single aggregate result",
                operationNameField(),
                stringField(KSMLDSL.TimeWindows.WINDOW_TYPE, "The type of the operation, either \"" + KSMLDSL.TimeWindows.TYPE_TUMBLING + "\", or \"" + KSMLDSL.TimeWindows.TYPE_HOPPING + "\", or \"" + KSMLDSL.TimeWindows.TYPE_SLIDING + "\""),
                optional(durationField(KSMLDSL.TimeWindows.DURATION, "(Tumbling) The duration of time windows")),
                optional(durationField(KSMLDSL.TimeWindows.ADVANCE_BY, "(Hopping) The amount of time to increase time windows by")),
                optional(durationField(KSMLDSL.TimeWindows.GRACE, "(Tumbling, Hopping, Sliding) The grace period, during which out-of-order records can still be processed")),
                optional(durationField(KSMLDSL.TimeWindows.TIME_DIFFERENCE, "(Sliding) The maximum amount of time difference between two records")),
                (name, windowType, duration, advanceBy, grace, timeDifference, tags) -> {
                    switch (windowType) {
                        case KSMLDSL.TimeWindows.TYPE_TUMBLING -> {
                            final var timeWindows = (grace != null && grace.toMillis() > 0)
                                    ? TimeWindows.ofSizeAndGrace(duration, grace)
                                    : TimeWindows.ofSizeWithNoGrace(duration);
                            return new WindowByTimeOperation(operationConfig(name, tags), timeWindows);
                        }
                        case KSMLDSL.TimeWindows.TYPE_HOPPING -> {
                            if (advanceBy.toMillis() > duration.toMillis()) {
                                throw new TopologyException("A hopping window can not advanceBy more than its duration");
                            }
                            final var timeWindows = (grace != null && grace.toMillis() > 0)
                                    ? org.apache.kafka.streams.kstream.TimeWindows.ofSizeAndGrace(duration, grace).advanceBy(advanceBy)
                                    : org.apache.kafka.streams.kstream.TimeWindows.ofSizeWithNoGrace(duration).advanceBy(advanceBy);
                            return new WindowByTimeOperation(operationConfig(name, tags), timeWindows);
                        }
                        case KSMLDSL.TimeWindows.TYPE_SLIDING -> {
                            final var slidingWindows = (grace != null && grace.toMillis() > 0)
                                    ? SlidingWindows.ofTimeDifferenceAndGrace(timeDifference, grace)
                                    : SlidingWindows.ofTimeDifferenceWithNoGrace(timeDifference);
                            return new WindowByTimeOperation(operationConfig(name, tags), slidingWindows);
                        }
                    }
                    throw new TopologyException("Unknown WindowType for windowByTime operation: " + windowType);
                });
    }
}
