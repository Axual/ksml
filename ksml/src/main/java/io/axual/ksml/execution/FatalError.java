package io.axual.ksml.execution;

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

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FatalError {

    public static final String NEW_LINE = System.lineSeparator();
    public static final String LOG_ITEM_SEPARATOR = "==========="+NEW_LINE;

    private FatalError() {
    }

    public static RuntimeException reportAndExit(Throwable t) {
        var messageBuilder = new StringBuilder().append(NEW_LINE);
        messageBuilder.append("FatalError").append(NEW_LINE).append(LOG_ITEM_SEPARATOR);
        printExceptionDetails(messageBuilder, t);
        log.error(messageBuilder.toString());
        System.exit(1);
        return new RuntimeException(t.getMessage());
    }

    private static void printExceptionDetails(StringBuilder messageBuilder, Throwable t) {
        if (t.getCause() != null) {
            printExceptionDetails(messageBuilder, t.getCause());
            messageBuilder.append(LOG_ITEM_SEPARATOR).append(NEW_LINE);
            messageBuilder.append("Above error caused: ").append(t.getMessage()).append(NEW_LINE);
        } else {
            messageBuilder.append("Description: ").append(t.getMessage()).append(NEW_LINE);
        }
        messageBuilder.append("Stack trace: ").append(NEW_LINE);
        for (var ste : t.getStackTrace()) {
            messageBuilder.append("  %s::%s @ %s:%s".formatted(ste.getClassName(), ste.getMethodName(), ste.getFileName(), ste.getLineNumber())).append(NEW_LINE);
        }
    }
}
