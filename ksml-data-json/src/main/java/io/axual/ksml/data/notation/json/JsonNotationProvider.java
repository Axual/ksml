package io.axual.ksml.data.notation.json;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library - JSON
 * %%
 * Copyright (C) 2021 - 2025 Axual B.V.
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

import io.axual.ksml.data.notation.Notation;
import io.axual.ksml.data.notation.NotationContext;
import io.axual.ksml.data.notation.NotationProvider;

/**
 * Provider/factory for the JSON notation.
 *
 * <p>This class is discovered/used by infrastructure that needs to obtain a
 * concrete {@link Notation} instance for a given {@link NotationContext}.
 * It exposes the canonical JSON notation name and constructs a {@link JsonNotation}
 * when requested.</p>
 */
public class JsonNotationProvider implements NotationProvider {
    /**
     * Returns the canonical notation name handled by this provider: {@link JsonNotation#NOTATION_NAME} ("json").
     */
    @Override
    public String notationName() {
        return JsonNotation.NOTATION_NAME;
    }

    /**
     * Creates a {@link JsonNotation} using the provided context. The context determines
     * the displayed name (including an optional vendor prefix), native mappers and serde configs.
     */
    @Override
    public Notation createNotation(NotationContext context) {
        // Delegate to JsonNotation which wires converter, schema loader, and default type
        return new JsonNotation(context);
    }
}
