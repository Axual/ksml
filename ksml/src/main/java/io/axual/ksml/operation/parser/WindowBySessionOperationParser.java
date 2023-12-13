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
import io.axual.ksml.parser.StructParser;
import org.apache.kafka.streams.kstream.SessionWindows;

public class WindowBySessionOperationParser extends OperationParser<WindowBySessionOperation> {
    public WindowBySessionOperationParser(TopologyResources resources) {
        super("windowBySession", resources);
    }

    @Override
    public StructParser<WindowBySessionOperation> parser() {
        return structParser(
                WindowBySessionOperation.class,
                "Operation to window messages by session, configured by an inactivity gap",
                stringField(KSMLDSL.Operations.TYPE_ATTRIBUTE, true, "The type of the operation, fixed value \"" + KSMLDSL.Operations.WINDOW_BY_TIME + "\""),
                nameField(),
                durationField(KSMLDSL.SessionWindows.INACTIVITY_GAP, true, "The inactivity gap, below which two messages are considered to be of the same session"),
                durationField(KSMLDSL.SessionWindows.GRACE, false, "(Tumbling, Hopping) The grace period, during which out-of-order records can still be processed"),
                (type, name, inactivityGap, grace) -> {
                    final var sessionWindows = (grace != null && grace.toMillis() > 0)
                            ? SessionWindows.ofInactivityGapAndGrace(inactivityGap, grace)
                            : SessionWindows.ofInactivityGapWithNoGrace(inactivityGap);
                    return new WindowBySessionOperation(operationConfig(name), sessionWindows);
                });
    }
}
