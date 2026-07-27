package io.axual.ksml.operation.parser;

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

import io.axual.ksml.data.mapper.DataObjectFlattener;
import io.axual.ksml.data.mapper.DataTypeFlattener;
import io.axual.ksml.data.notation.NotationContext;
import io.axual.ksml.data.notation.binary.BinaryNotation;
import io.axual.ksml.data.notation.json.JsonNotation;
import io.axual.ksml.execution.ExecutionContext;
import io.axual.ksml.parser.ParseNode;
import io.axual.ksml.parser.ParseTestSupport;
import io.axual.ksml.type.UserType;

/**
 * Shared helpers for operation parser tests: notation registration and YAML-to-{@link ParseNode} conversion.
 */
final class OperationParserTestSupport {

    private OperationParserTestSupport() {
    }

    static void registerNotations() {
        final var jsonNotation = new JsonNotation(new NotationContext(new DataObjectFlattener(), new DataTypeFlattener()));
        ExecutionContext.INSTANCE.notationLibrary().register(UserType.DEFAULT_NOTATION,
                new BinaryNotation(new NotationContext(new DataObjectFlattener(), new DataTypeFlattener()), jsonNotation::serde));
        ExecutionContext.INSTANCE.notationLibrary().register(JsonNotation.NOTATION_NAME, jsonNotation);
    }

    static ParseNode nodeOf(String yaml) throws Exception {
        return ParseTestSupport.nodeOf(yaml);
    }
}
