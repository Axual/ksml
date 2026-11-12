package io.axual.ksml.testutil;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2026 Axual B.V.
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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

/**
 * Captures what code logs. slf4j-simple writes to {@link System#err}, so tests that use this must not run in parallel.
 */
public final class LogCapture {
    private LogCapture() {
    }

    /** An action that can throw any exception. */
    public interface ThrowingRunnable {
        void run() throws Exception;
    }

    /**
     * Runs the action and returns what it wrote to {@link System#err}.
     *
     * @param action the action to run
     * @return the captured output
     * @throws Exception when the action throws
     */
    public static String stderrOf(ThrowingRunnable action) throws Exception {
        final var originalErr = System.err;
        final var captured = new ByteArrayOutputStream();
        System.setErr(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try {
            action.run();
        } finally {
            System.setErr(originalErr);
        }
        return captured.toString(StandardCharsets.UTF_8);
    }
}
