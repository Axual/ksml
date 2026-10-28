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

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * Writes the outcome of every parity case to {@code target/parity-report}: one detail file per case and
 * optimization setting, and one line per run in {@code summary.tsv}.
 */
@Slf4j
final class ParityReport {
    private static final Path DIR = Paths.get("target",
            "docker".equals(System.getProperty(ParityCase.MODE_PROPERTY)) ? "parity-report-docker" : "parity-report");

    private ParityReport() {
    }

    static synchronized void write(ParityCase parityCase, ParityComparison comparison) {
        final var details = comparison.details();
        log.info("\n{}", details);
        try {
            Files.createDirectories(DIR);
            final var firstDiff = comparison.differences().isEmpty() ? "" : comparison.differences().getFirst().replaceAll("\\s+", " ");
            final var line = String.join("\t", parityCase.id, comparison.optimization(), comparison.status(),
                    parityCase.title, firstDiff) + "\n";
            Files.writeString(DIR.resolve("summary.tsv"), line, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            final var full = details
                    + "\n--- kafka streams topology\n" + comparison.reference().topologyDescription
                    + "\n--- ksml topology\n" + comparison.ksml().topologyDescription
                    + "\n--- kafka streams output\n" + comparison.reference().records
                    + "\n--- ksml output\n" + comparison.ksml().records
                    + "\n--- kafka streams stores\n" + comparison.reference().stores
                    + "\n--- ksml stores\n" + comparison.ksml().stores
                    + "\n--- kafka streams produced topics\n" + comparison.reference().producedTopics
                    + "\n--- ksml produced topics\n" + comparison.ksml().producedTopics + "\n";
            Files.writeString(DIR.resolve(parityCase.id + "-" + comparison.optimization() + ".txt"), full);
        } catch (IOException e) {
            log.warn("Could not write parity report for {}", parityCase.id, e);
        }
    }
}
