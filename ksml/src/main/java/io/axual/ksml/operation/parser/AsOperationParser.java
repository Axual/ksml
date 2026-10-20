package io.axual.ksml.operation.parser;

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

import io.axual.ksml.exception.ParseException;
import io.axual.ksml.parser.FieldParsers;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.operation.AsOperation;
import io.axual.ksml.parser.StructsParser;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AsOperationParser extends OperationParser<AsOperation> {
    public AsOperationParser(TopologyResources resources) {
        super(KSMLDSL.Operations.AS, resources);
    }

    public StructsParser<AsOperation> parser() {
        final var parser = FieldParsers.structsParser(
                AsOperation.class,
                "",
                "An operation to close the pipeline and save the result under a given name",
                FieldParsers.stringField(KSMLDSL.Operations.AS, "The name to register the pipeline result under, which can be used as source by follow-up pipelines"),
                (name, tags) -> name != null ? new AsOperation(operationConfig(name, tags), name) : null);
        return StructsParser.of(node -> {
            final var result = parser.parse(node);
            if (result == null) return null;
            final var name = result.targetName;
            // A "from" with this name gets the saved result, not the defined one. Only a warning, so existing
            // definitions keep working
            final var section = resources().sectionOf(name);
            if (section != null) {
                log.warn("\"as: {}\" is also under \"{}\": \"from\" gets the saved result, joins and merges get the defined one. Use another name", name, section);
            }
            final var earlier = resources().pipelineSaving(name);
            if (earlier != null) throw new ParseException(node, "\"as: " + name + "\" is already used by " + earlier + ". Use another name");
            resources().registerSavedName(name, node.tags());
            return result;
        }, parser.schemas());
    }
}
