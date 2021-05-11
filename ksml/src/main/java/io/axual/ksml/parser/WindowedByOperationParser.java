package io.axual.ksml.parser;

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

import static io.axual.ksml.dsl.KSMLDSL.WINDOWEDBY_WINDOWTYPE_ATTRIBUTE;
import static io.axual.ksml.dsl.KSMLDSL.WINDOWEDBY_WINDOWTYPE_SESSION;
import static io.axual.ksml.dsl.KSMLDSL.WINDOWEDBY_WINDOWTYPE_SESSION_INACTIVITYGAP;
import static io.axual.ksml.dsl.KSMLDSL.WINDOWEDBY_WINDOWTYPE_SLIDING;
import static io.axual.ksml.dsl.KSMLDSL.WINDOWEDBY_WINDOWTYPE_SLIDING_GRACE;
import static io.axual.ksml.dsl.KSMLDSL.WINDOWEDBY_WINDOWTYPE_SLIDING_TIMEDIFFERENCE;
import static io.axual.ksml.dsl.KSMLDSL.WINDOWEDBY_WINDOWTYPE_TIME;
import static io.axual.ksml.dsl.KSMLDSL.WINDOWEDBY_WINDOWTYPE_TIME_DURATION;

public class WindowedByOperationParser extends ContextAwareParser<WindowedByOperation> {
    protected WindowedByOperationParser(ParseContext context) {
        super(context);
    }

    @Override
    public WindowedByOperation parse(YamlNode node) {
        if (node == null) return null;
        String windowType = parseText(node, WINDOWEDBY_WINDOWTYPE_ATTRIBUTE);
        if (windowType != null) {
            switch (windowType) {
                case WINDOWEDBY_WINDOWTYPE_SESSION:
                    return new WindowedByOperation(SessionWindows.with(parseDuration(node, WINDOWEDBY_WINDOWTYPE_SESSION_INACTIVITYGAP)));
                case WINDOWEDBY_WINDOWTYPE_SLIDING:
                    return new WindowedByOperation(SlidingWindows.withTimeDifferenceAndGrace(
                            parseDuration(node, WINDOWEDBY_WINDOWTYPE_SLIDING_TIMEDIFFERENCE),
                            parseDuration(node, WINDOWEDBY_WINDOWTYPE_SLIDING_GRACE)));
                case WINDOWEDBY_WINDOWTYPE_TIME:
                    return new WindowedByOperation(TimeWindows.of(parseDuration(node, WINDOWEDBY_WINDOWTYPE_TIME_DURATION)));
                default:
                    throw new KSMLParseException(node, "Unknown WindowType for windowedBy operation: " + windowType);
            }
        }
        throw new KSMLParseException(node, "WindowType missing for windowedBy operation");
    }
}
