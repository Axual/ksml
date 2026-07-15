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

import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.definition.StreamDefinition;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.operation.ToOperation;
import io.axual.ksml.operation.ToTopicNameExtractorOperation;
import io.axual.ksml.type.UserType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.axual.ksml.operation.parser.OperationParserTestSupport.nodeOf;
import static io.axual.ksml.operation.parser.OperationParserTestSupport.registerNotations;
import static org.assertj.core.api.Assertions.assertThat;

class ToOperationParsersTest {

    private final TopologyResources resources = new TopologyResources("test");

    @BeforeEach
    void setUp() {
        registerNotations();
        resources.register("outStream",
                new StreamDefinition("out_topic", UserType.UNKNOWN, UserType.UNKNOWN, null, null, null));
        resources.register("myExtractor", FunctionDefinition.as(
                "generic", "myExtractor", List.of(), (String) null, (String) null, "topic", new UserType(io.axual.ksml.data.object.DataString.DATATYPE), List.of()));
    }

    @Test
    @DisplayName("a to operation referencing a registered topic parses into a to operation")
    void parsesToWithTopicReference() throws Exception {
        assertThat(new ToOperationParser(resources).parser().parse(nodeOf("to: outStream")))
                .isInstanceOf(ToOperation.class);
    }

    @Test
    @DisplayName("a to operation with an inline topic definition parses into a to operation")
    void parsesToWithInlineTopic() throws Exception {
        assertThat(new ToOperationParser(resources).parser()
                .parse(nodeOf("to:\n  topic: inline_out\n  keyType: string\n  valueType: string")))
                .isInstanceOf(ToOperation.class);
    }

    @Test
    @DisplayName("a toTopicNameExtractor referencing a function parses into a to-topic-name-extractor operation")
    void parsesToTopicNameExtractorWithFunctionReference() throws Exception {
        assertThat(new ToTopicNameExtractorOperationParser(resources).parser()
                .parse(nodeOf("toTopicNameExtractor: myExtractor")))
                .isInstanceOf(ToTopicNameExtractorOperation.class);
    }
}
