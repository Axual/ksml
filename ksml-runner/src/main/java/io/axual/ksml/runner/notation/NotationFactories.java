package io.axual.ksml.runner.notation;

/*-
 * ========================LICENSE_START=================================
 * KSML Runner
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

import io.axual.ksml.client.util.MapUtil;
import io.axual.ksml.data.mapper.DataObjectFlattener;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.notation.Notation;
import io.axual.ksml.data.notation.NotationContext;
import io.axual.ksml.data.notation.NotationProvider;
import io.axual.ksml.data.notation.binary.BinaryNotation;
import io.axual.ksml.data.notation.json.JsonNotation;
import io.axual.ksml.data.notation.json.JsonNotationProvider;
import io.axual.ksml.type.UserType;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

@Getter
public class NotationFactories {
    public interface NotationFactory {
        Notation create(Map<String, String> notationConfig);
    }

    // Notation constants
    private final Map<String, NotationFactory> notations = new HashMap<>();

    public NotationFactories(Map<String, String> kafkaConfig) {
        final var nativeMapper = new DataObjectFlattener();

        // Load all notations
        for (final var provider : ServiceLoader.load(NotationProvider.class)) {
            final var context = new NotationContext(provider.notationName(), provider.vendorName(), nativeMapper, kafkaConfig);
            notations.put(context.name(), configs ->
                    provider.createNotation(
                            new NotationContext(
                                    context.notationName(),
                                    context.vendorName(),
                                    context.nativeDataObjectMapper(),
                                    MapUtil.merge(context.serdeConfigs(), configs))));
        }

        // JSON and Binary notations are core to KSML, so we instantiate them manually

        // JSON notation
        final var jsonContext = new NotationContext(JsonNotation.NOTATION_NAME, nativeMapper);
        final NotationFactory json = configs -> new JsonNotationProvider().createNotation(jsonContext);
        notations.put(jsonContext.name(), json);

        // Create the binary notation, with JSON for complex types
        final var binaryContext = new NotationContext(BinaryNotation.NOTATION_NAME, nativeMapper);
        notations.put(binaryContext.name(), config -> new BinaryNotation(binaryContext, json.create(null)::serde));

        // Create the binary notation, with JSON for complex types and register it as default notation
        final var defaultContext = new NotationContext(UserType.DEFAULT_NOTATION, nativeMapper);
        notations.put(defaultContext.name(), config -> new BinaryNotation(defaultContext, json.create(null)::serde));
    }
}
