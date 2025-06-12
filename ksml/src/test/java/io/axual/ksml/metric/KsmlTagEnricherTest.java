package io.axual.ksml.metric;

/*-
 * ========================LICENSE_START=================================
 * KSML
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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.regex.Matcher;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class KsmlTagEnricherPatternTest {

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("examples")
    void nodePatternParsesCorrectly(String nodeName,
                                    String expectedNamespace,
                                    String expectedPipelineName,
                                    String expectedStepNumber,
                                    String expectedOperationName) {

        Matcher m = KsmlTagEnricher.NODE_PATTERN.matcher(nodeName);

        assertThat(m.matches())
                .as("Pattern should match: %s", nodeName)
                .isTrue();

        assertThat(m.group("ns"))
                .as("namespace")
                .isEqualTo(expectedNamespace);

        assertThat(m.group("pipeline"))
                .as("pipeline")
                .isEqualTo(expectedPipelineName);

        assertThat(m.group("step"))
                .as("step")
                .isEqualTo(expectedStepNumber);

        assertThat(m.group("op"))
                .as("operation")
                .isEqualTo(expectedOperationName);
    }

    private static Stream<Arguments> examples() {
        return Stream.of(
                Arguments.of("inspect_pipelines_consume_avro_forEach",
                        "inspect", "consume_avro",            null, "forEach"),
                Arguments.of("copy_pipelines_main_copy_pipeline_via_step1_peek",
                        "copy",    "main_copy_pipeline",      "1",  "peek"),
                Arguments.of("copy_pipelines_main_copy_pipeline_to",
                        "copy",    "main_copy_pipeline",      null, "to"),
                Arguments.of("duplicate_pipelines_main_via_step1_transformKeyValueToKeyValueList",
                        "duplicate","main",                   "1",  "transformKeyValueToKeyValueList"),
                Arguments.of("convert_pipelines_main_to_xml_via_step2_peek",
                        "convert", "main_to_xml",             "2",  "peek"),
                Arguments.of("convert_pipelines_alternate_to_xml_to",
                        "convert", "alternate_to_xml",        null, "to"),
                Arguments.of("route_pipelines_main_toTopicNameExtractor",
                        "route",   "main",                    null, "toTopicNameExtractor"),
                Arguments.of("queryable_table_pipelines_main_via_step2_peek",
                        "queryable_table","main",             "2",  "peek"),
                Arguments.of("convert_pipelines_main_to_via_xml_via_step6_peek",
                        "convert", "main_to_via_xml",         "6",  "peek"),
                Arguments.of("via_convert_main_pipelines_main_to_via_xml_via_step6_peek",
                        "via_convert_main","main_to_via_xml", "6",  "peek")
        );
    }

    @ParameterizedTest
    @MethodSource("invalidNodeNames")
    void shouldNotMatchInvalidNodeNames(String invalidNodeName) {
        Matcher m = KsmlTagEnricher.NODE_PATTERN.matcher(invalidNodeName);
        assertThat(m.matches())
                .as("Pattern should not match: %s", invalidNodeName)
                .isFalse();
    }

    private static Stream<String> invalidNodeNames() {
        return Stream.of(
                "invalid_format",
                "missing_pipelines_part",
                "namespace_pipeline_operation", // missing 'pipelines'
                "_pipelines_pipeline_op", // empty namespace
                "ns_pipelines__op", // empty pipeline
                "ns_pipelines_pipe_" // empty operation
        );
    }

    @Test
    void shouldHandleSpecialCharactersInNodeNames() {
        // Test with special characters that might break regex
        String nodeWithSpecialChars = "ns-with-dash_pipelines_pipe.with.dots_op";
        Matcher m = KsmlTagEnricher.NODE_PATTERN.matcher(nodeWithSpecialChars);

        assertThat(m.matches()).isTrue();
        assertThat(m.group("ns")).isEqualTo("ns-with-dash");
        assertThat(m.group("pipeline")).isEqualTo("pipe.with.dots");
        assertThat(m.group("op")).isEqualTo("op");
    }
}
