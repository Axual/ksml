package io.axual.ksml.definition.parser;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2024 Axual B.V.
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
import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy;
import org.apache.kafka.streams.AutoOffsetReset;

public class OffsetResetPolicyParser {
    public static AutoOffsetReset parseResetPolicy(String resetPolicy) {
        if (resetPolicy == null || resetPolicy.isEmpty()) return null;
        final var strategy = AutoOffsetResetStrategy.fromString(resetPolicy);
        if (strategy.type() == AutoOffsetResetStrategy.StrategyType.EARLIEST)
            return AutoOffsetReset.earliest();
        if (strategy.type() == AutoOffsetResetStrategy.StrategyType.LATEST)
            return AutoOffsetReset.latest();
        if (strategy.type() == AutoOffsetResetStrategy.StrategyType.NONE)
            return AutoOffsetReset.none();
        if (strategy.type() == AutoOffsetResetStrategy.StrategyType.BY_DURATION && strategy.duration().isPresent())
            return AutoOffsetReset.byDuration(strategy.duration().get());
        throw new TopologyException("Unknown offset reset policy: " + resetPolicy);
    }

    private OffsetResetPolicyParser() {
        // Prevent instantiation.
    }
}
