package io.axual.ksml.parity;

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

import io.axual.ksml.testutil.KSMLTestExtension;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * Not a parity check: each definition holds a mistake that users often make. The test records what KSML
 * reports for it (when, and with which message) in {@code target/parity-report/usability.txt}.
 */
@ExtendWith(KSMLTestExtension.class)
class UsabilityProbeParityTest {
    // A probe only records what KSML reports for a mistake, so there is nothing to assert
    @SuppressWarnings("java:S2699")
    @ParameterizedTest
    @ValueSource(strings = {
            "u01-typo-in-operation-type", "u02-unknown-stream-in-to", "u03-unknown-field", "u04-python-syntax-error",
            "u05-result-type-mismatch", "u06-grouped-stream-to-topic", "u07-stream-join-without-window",
            "u08-count-into-json-topic", "u09-key-in-reducer", "u10-windowed-key-into-string-topic",
            "u11-misspelled-optional-field", "u12-misspelled-store-option", "u13-fk-extractor-on-stream"})
    void probe(String name) throws IOException {
        Assumptions.assumeFalse("docker".equals(System.getProperty(ParityCase.MODE_PROPERTY)), "test driver only");
        final var parityCase = ParityCase.of(name, name)
                .input("in").output("out")
                .ksml("parity/usability/" + name + ".yaml")
                .pipe("in", "k1", "{\"amount\":5}", 1000)
                .pipe("in", "k1", "{\"amount\":6}", 2000);
        final var result = ParityRunner.runKsml(parityCase, "all");
        final String outcome;
        if (result.buildError != null) outcome = "FAILS WHEN LOADED: " + result.buildError;
        else if (result.processingError != null) outcome = "FAILS ON A RECORD: " + result.processingError;
        else outcome = "RUNS, output: " + result.records;
        Files.createDirectories(Paths.get("target", "parity-report"));
        Files.writeString(Paths.get("target", "parity-report", "usability.txt"), name + "\n  " + outcome + "\n",
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }
}
