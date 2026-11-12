package io.axual.ksml.definition.parser;

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

import io.axual.ksml.definition.PipelineDefinition;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.parser.ParseTestSupport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A pipeline with no explicit {@code name:} field and no terminal/sink operation (as/branch/
 * forEach/print/to/toTopicNameExtractor) relies on an implicitly-generated AS operation so that
 * other pipelines can refer to it by name (see the comment in {@link PipelineDefinitionParser#parser()}).
 * This pins that the resulting {@link PipelineDefinition}'s name is the pipeline's own short
 * (map-key-derived) identifier — not {@code null}, and not the long/qualified internal name used
 * for Kafka Streams node naming.
 */
class PipelineDefinitionParserTest {
    private final TopologyResources resources = new TopologyResources("test");

    @Test
    @DisplayName("A pipeline with no explicit name and no sink operation gets its short (map-key) name, not null")
    void namelessSinkLessPipelineGetsDerivedShortName() throws Exception {
        final var parser = new PipelineDefinitionParser(resources, false);
        parser.defaultShortName("my_pipeline");
        parser.defaultLongName("test_pipelines_my_pipeline");

        final var definition = parser.parser().parse(ParseTestSupport.nodeOf("{}"));

        assertThat(definition).isInstanceOf(PipelineDefinition.class);
        assertThat(definition.name()).isEqualTo("my_pipeline");
    }
}
