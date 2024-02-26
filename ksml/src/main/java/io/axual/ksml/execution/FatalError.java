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

import io.axual.ksml.exception.TopologyException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FatalError {
    private FatalError() {
    }

    public static Throwable topologyError(String message) {
        return reportAndExit(new TopologyException(message));
    }

    public static RuntimeException reportAndExit(Throwable t) {
        log.error("\n\n");
        log.error("Fatal error");
        printLineSeparator();
        printExceptionDetails(t);
        System.exit(1);
        // Dummy return to resolve compiler errors
        return new RuntimeException(t.getMessage());
    }

    private static void printExceptionDetails(Throwable t) {
        if (t.getCause() != null) {
            printExceptionDetails(t.getCause());
            printLineSeparator();
            log.error("Above error caused: {}", t.getMessage());
        } else {
            log.error("Description: {}", t.getMessage());
        }
        log.error("Stack trace:");
        for (var ste : t.getStackTrace()) {
            log.error("  {}::{} @ {}:{}", ste.getClassName(), ste.getMethodName(), ste.getFileName(), ste.getLineNumber());
        }
    }

    private static void printLineSeparator() {
        log.error("===========");
    }
}
