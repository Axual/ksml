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


import io.axual.ksml.operation.WindowBySessionOperation;
import io.axual.ksml.parser.ParseContext;
import io.axual.ksml.parser.YamlNode;
import org.apache.kafka.streams.kstream.SessionWindows;

import static io.axual.ksml.dsl.KSMLDSL.WINDOWEDBY_WINDOWTYPE_SESSION_GRACE;
import static io.axual.ksml.dsl.KSMLDSL.WINDOWEDBY_WINDOWTYPE_SESSION_INACTIVITYGAP;

public class WindowBySessionOperationParser extends OperationParser<WindowBySessionOperation> {
    private final String name;

    protected WindowBySessionOperationParser(String name, ParseContext context) {
        super(context);
        this.name = name;
    }

    @Override
    public WindowBySessionOperation parse(YamlNode node) {
        if (node == null) return null;
        final var duration = parseDuration(node, WINDOWEDBY_WINDOWTYPE_SESSION_INACTIVITYGAP, "Missing inactivityGap attribute for session window");
        final var grace = parseDuration(node, WINDOWEDBY_WINDOWTYPE_SESSION_GRACE);
        final var sessionWindows = (grace != null && grace.toMillis() > 0)
                ? SessionWindows.ofInactivityGapAndGrace(duration, grace)
                : SessionWindows.ofInactivityGapWithNoGrace(duration);
        return new WindowBySessionOperation(parseConfig(node, name), sessionWindows);
    }
}
