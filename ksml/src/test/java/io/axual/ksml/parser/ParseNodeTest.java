package io.axual.ksml.parser;

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

import io.axual.ksml.metric.MetricTag;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.axual.ksml.parser.ParseTestSupport.nodeOf;
import static org.assertj.core.api.Assertions.assertThat;

class ParseNodeTest {

    private static List<String> tagKeys(ParseNode node) {
        return node.tags().stream().map(MetricTag::key).toList();
    }

    @Test
    @DisplayName("A child gets the tag key unchanged when no parent has it")
    void childGetsTagKey() throws Exception {
        final var child = nodeOf("branch: [x]").get("branch").children("branch", "branch").getFirst();

        assertThat(tagKeys(child)).containsExactly("namespace", "branch");
    }

    @Test
    @DisplayName("A tag key that a parent already has gets a level number")
    void repeatedTagKeyGetsLevelNumber() throws Exception {
        final var level1 = nodeOf("branch: [{branch: [{branch: [x]}]}]").get("branch").children("branch", "branch").getFirst();
        final var level2 = level1.get("branch").children("branch", "branch").getFirst();
        final var level3 = level2.get("branch").children("branch", "branch").getFirst();

        assertThat(tagKeys(level3)).containsExactly("namespace", "branch", "branch_2", "branch_3");
    }

    @Test
    @DisplayName("Without a tag key, a child keeps the tags of its parent")
    void noTagKeyKeepsTags() throws Exception {
        final var child = nodeOf("list: [x]").get("list").children(null, "").getFirst();

        assertThat(tagKeys(child)).containsExactly("namespace");
    }
}
