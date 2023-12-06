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
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.operation.WindowBySessionOperation;
import io.axual.ksml.parser.YamlNode;
import org.apache.kafka.streams.kstream.SessionWindows;

public class WindowBySessionOperationParser extends OperationParser<WindowBySessionOperation> {
    public WindowBySessionOperationParser(String prefix, String name, TopologyResources resources) {
        super(prefix, name, resources);
    }

    @Override
    public WindowBySessionOperation parse(YamlNode node) {
        if (node == null) return null;
        final var duration = parseDuration(node, KSMLDSL.SessionWindows.INACTIVITY_GAP, "Missing inactivityGap attribute for session window");
        final var grace = parseDuration(node, KSMLDSL.SessionWindows.GRACE);
        final var sessionWindows = (grace != null && grace.toMillis() > 0)
                ? SessionWindows.ofInactivityGapAndGrace(duration, grace)
                : SessionWindows.ofInactivityGapWithNoGrace(duration);
        return new WindowBySessionOperation(operationConfig(node), sessionWindows);
    }
}
