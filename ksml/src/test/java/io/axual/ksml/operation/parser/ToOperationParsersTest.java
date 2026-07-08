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

import com.fasterxml.jackson.databind.JsonNode;
import io.axual.ksml.data.mapper.DataObjectFlattener;
import io.axual.ksml.data.mapper.DataTypeFlattener;
import io.axual.ksml.data.notation.NotationContext;
import io.axual.ksml.data.notation.binary.BinaryNotation;
import io.axual.ksml.data.notation.json.JsonNotation;
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.definition.StreamDefinition;
import io.axual.ksml.execution.ExecutionContext;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.generator.YAMLObjectMapper;
import io.axual.ksml.operation.ToOperation;
import io.axual.ksml.operation.ToTopicNameExtractorOperation;
import io.axual.ksml.parser.ParseNode;
import io.axual.ksml.type.UserType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ToOperationParsersTest {

    private final TopologyResources resources = new TopologyResources("test");

    @BeforeEach
    void setUp() {
        final var jsonNotation = new JsonNotation(new NotationContext(new DataObjectFlattener(), new DataTypeFlattener()));
        ExecutionContext.INSTANCE.notationLibrary().register(UserType.DEFAULT_NOTATION,
                new BinaryNotation(new NotationContext(new DataObjectFlattener(), new DataTypeFlattener()), jsonNotation::serde));
        ExecutionContext.INSTANCE.notationLibrary().register(JsonNotation.NOTATION_NAME, jsonNotation);
        resources.register("outStream",
                new StreamDefinition("out_topic", UserType.UNKNOWN, UserType.UNKNOWN, null, null, null));
        resources.register("myExtractor", FunctionDefinition.as(
                "generic", "myExtractor", List.of(), (String) null, (String) null, "topic", new UserType(io.axual.ksml.data.object.DataString.DATATYPE), List.of()));
    }

    private static ParseNode nodeOf(String yaml) throws Exception {
        return ParseNode.fromRoot(YAMLObjectMapper.INSTANCE.readValue(yaml, JsonNode.class), "test");
    }

    @Test
    void parsesToWithTopicReference() throws Exception {
        assertThat(new ToOperationParser(resources).parser().parse(nodeOf("to: outStream")))
                .isInstanceOf(ToOperation.class);
    }

    @Test
    void parsesToWithInlineTopic() throws Exception {
        assertThat(new ToOperationParser(resources).parser()
                .parse(nodeOf("to:\n  topic: inline_out\n  keyType: string\n  valueType: string")))
                .isInstanceOf(ToOperation.class);
    }

    @Test
    void parsesToTopicNameExtractorWithFunctionReference() throws Exception {
        assertThat(new ToTopicNameExtractorOperationParser(resources).parser()
                .parse(nodeOf("toTopicNameExtractor: myExtractor")))
                .isInstanceOf(ToTopicNameExtractorOperation.class);
    }
}
