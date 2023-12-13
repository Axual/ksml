package io.axual.ksml.definition.parser;

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


import io.axual.ksml.definition.ParameterDefinition;
import io.axual.ksml.parser.DefinitionParser;
import io.axual.ksml.parser.StructParser;

import static io.axual.ksml.dsl.KSMLDSL.Functions;

public class ParameterDefinitionParser extends DefinitionParser<ParameterDefinition> {
    @Override
    public StructParser<ParameterDefinition> parser() {
        return structParser(ParameterDefinition.class,
                "Defines a parameter for a user function",
                stringField(Functions.Parameters.NAME, true, null, "The name of the parameter"),
                userTypeField(Functions.Parameters.TYPE, true, "The type of the parameter"),
                stringField(Functions.Parameters.DEFAULT_VALUE, false, "The default value for the parameter"),
                (name, type, defaultValue) -> new ParameterDefinition(name, type.dataType(), true, defaultValue));
    }
}
