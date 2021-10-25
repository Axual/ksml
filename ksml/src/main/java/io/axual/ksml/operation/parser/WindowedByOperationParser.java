package io.axual.ksml.operation.parser;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 Axual B.V.
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


import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.TimeWindows;

import io.axual.ksml.exception.KSMLParseException;
import io.axual.ksml.operation.WindowedByOperation;
import io.axual.ksml.parser.ContextAwareParser;
import io.axual.ksml.parser.ParseContext;
import io.axual.ksml.parser.YamlNode;

import static io.axual.ksml.dsl.KSMLDSL.WINDOWEDBY_WINDOWTYPE_ATTRIBUTE;
import static io.axual.ksml.dsl.KSMLDSL.WINDOWEDBY_WINDOWTYPE_SESSION;
import static io.axual.ksml.dsl.KSMLDSL.WINDOWEDBY_WINDOWTYPE_SESSION_INACTIVITYGAP;
import static io.axual.ksml.dsl.KSMLDSL.WINDOWEDBY_WINDOWTYPE_SLIDING;
import static io.axual.ksml.dsl.KSMLDSL.WINDOWEDBY_WINDOWTYPE_SLIDING_GRACE;
import static io.axual.ksml.dsl.KSMLDSL.WINDOWEDBY_WINDOWTYPE_SLIDING_TIMEDIFFERENCE;
import static io.axual.ksml.dsl.KSMLDSL.WINDOWEDBY_WINDOWTYPE_TIME;
import static io.axual.ksml.dsl.KSMLDSL.WINDOWEDBY_WINDOWTYPE_TIME_ADVANCEBY;
import static io.axual.ksml.dsl.KSMLDSL.WINDOWEDBY_WINDOWTYPE_TIME_DURATION;
import static io.axual.ksml.dsl.KSMLDSL.WINDOWEDBY_WINDOWTYPE_TIME_GRACE;

public class WindowedByOperationParser extends ContextAwareParser<WindowedByOperation> {
    private final String name;

    protected WindowedByOperationParser(String name, ParseContext context) {
        super(context);
        this.name = name;
    }

    @Override
    public WindowedByOperation parse(YamlNode node) {
        if (node == null) return null;
        String windowType = parseText(node, WINDOWEDBY_WINDOWTYPE_ATTRIBUTE);
        if (windowType != null) {
            switch (windowType) {
                case WINDOWEDBY_WINDOWTYPE_SESSION:
                    return parseSessionWindows(node);
                case WINDOWEDBY_WINDOWTYPE_SLIDING:
                    return parseSlidingWindows(node);
                case WINDOWEDBY_WINDOWTYPE_TIME:
                    return parseTimeWindows(node);
                default:
                    throw new KSMLParseException(node, "Unknown WindowType for windowedBy operation: " + windowType);
            }
        }
        throw new KSMLParseException(node, "WindowType missing for windowedBy operation");
    }

    private WindowedByOperation parseSessionWindows(YamlNode node) {
        var duration = parseDuration(node, WINDOWEDBY_WINDOWTYPE_SESSION_INACTIVITYGAP);
        var grace = parseDuration(node, WINDOWEDBY_WINDOWTYPE_SESSION_INACTIVITYGAP);
        var sessionWindows = SessionWindows.with(duration);
        if (grace != null && grace.toMillis() > 0) {
            sessionWindows = sessionWindows.grace(grace);
        }
        return new WindowedByOperation(operationConfig(name), sessionWindows);
    }

    private WindowedByOperation parseSlidingWindows(YamlNode node) {
        var timeDifference = parseDuration(node, WINDOWEDBY_WINDOWTYPE_SLIDING_TIMEDIFFERENCE);
        var grace = parseDuration(node, WINDOWEDBY_WINDOWTYPE_SLIDING_GRACE);
        return new WindowedByOperation(operationConfig(name), SlidingWindows.withTimeDifferenceAndGrace(timeDifference, grace));
    }

    private WindowedByOperation parseTimeWindows(YamlNode node) {
        var duration = parseDuration(node, WINDOWEDBY_WINDOWTYPE_TIME_DURATION);
        var advanceBy = parseDuration(node, WINDOWEDBY_WINDOWTYPE_TIME_ADVANCEBY);
        var grace = parseDuration(node, WINDOWEDBY_WINDOWTYPE_TIME_GRACE);
        var timeWindows = TimeWindows.of(duration);
        if (advanceBy != null && advanceBy.toMillis() > 0 && advanceBy.toMillis() <= duration.toMillis()) {
            timeWindows = timeWindows.advanceBy(advanceBy);
        }
        if (grace != null && grace.toMillis() > 0) {
            timeWindows = timeWindows.grace(grace);
        }
        return new WindowedByOperation(operationConfig(name), timeWindows);
    }
}
