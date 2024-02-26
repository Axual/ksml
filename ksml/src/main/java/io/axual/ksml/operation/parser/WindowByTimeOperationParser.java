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
import io.axual.ksml.parser.StructParser;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.TimeWindows;

public class WindowByTimeOperationParser extends OperationParser<WindowByTimeOperation> {
    public WindowByTimeOperationParser(TopologyResources resources) {
        super("windowByTime", resources);
    }

    @Override
    public StructParser<WindowByTimeOperation> parser() {
        return structParser(
                WindowByTimeOperation.class,
                "Operation to reduce a series of records into a single aggregate result",
                operationTypeField(KSMLDSL.Operations.WINDOW_BY_TIME),
                nameField(),
                stringField(KSMLDSL.TimeWindows.WINDOW_TYPE, true, "The type of the operation, either \"" + KSMLDSL.TimeWindows.TYPE_TUMBLING + "\", or \"" + KSMLDSL.TimeWindows.TYPE_HOPPING + "\", or \"" + KSMLDSL.TimeWindows.TYPE_SLIDING + "\""),
                durationField(KSMLDSL.TimeWindows.DURATION, true, "(Tumbling) The duration of time windows"),
                durationField(KSMLDSL.TimeWindows.ADVANCE_BY, true, "(Hopping) The amount of time to increase time windows by"),
                durationField(KSMLDSL.TimeWindows.GRACE, false, "(Tumbling, Hopping) The grace period, during which out-of-order records can still be processed"),
                durationField(KSMLDSL.TimeWindows.TIME_DIFFERENCE, true, "(Sliding) The maximum amount of time difference between two records"),
                (type, name, windowType, duration, advanceBy, grace, timeDifference) -> {
                    switch (windowType) {
                        case KSMLDSL.TimeWindows.TYPE_TUMBLING -> {
                            final var timeWindows = (grace != null && grace.toMillis() > 0)
                                    ? TimeWindows.ofSizeAndGrace(duration, grace)
                                    : TimeWindows.ofSizeWithNoGrace(duration);
                            return new WindowByTimeOperation(operationConfig(name), timeWindows);
                        }
                        case KSMLDSL.TimeWindows.TYPE_HOPPING -> {
                            if (advanceBy.toMillis() > duration.toMillis()) {
                                throw new TopologyException("A hopping window can not advanceBy more than its duration");
                            }
                            final var timeWindows = (grace != null && grace.toMillis() > 0)
                                    ? org.apache.kafka.streams.kstream.TimeWindows.ofSizeAndGrace(duration, grace).advanceBy(advanceBy)
                                    : org.apache.kafka.streams.kstream.TimeWindows.ofSizeWithNoGrace(duration).advanceBy(advanceBy);
                            return new WindowByTimeOperation(operationConfig(name), timeWindows);
                        }
                        case KSMLDSL.TimeWindows.TYPE_SLIDING -> {
                            final var slidingWindows = (grace != null && grace.toMillis() > 0)
                                    ? SlidingWindows.ofTimeDifferenceAndGrace(timeDifference, grace)
                                    : SlidingWindows.ofTimeDifferenceWithNoGrace(timeDifference);
                            return new WindowByTimeOperation(operationConfig(name), slidingWindows);
                        }
                    }
                    throw new TopologyException("Unknown WindowType for windowByTime operation: " + windowType);
                });
    }
}
