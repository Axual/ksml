package io.axual.ksml.parser;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 Axual B.V.
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



import io.axual.ksml.operation.BranchOperation;
import io.axual.ksml.operation.ForEachOperation;
import io.axual.ksml.operation.ToOperation;
import io.axual.ksml.operation.ToTopicNameExtractorOperation;

import static io.axual.ksml.dsl.KSMLDSL.PIPELINE_BRANCH_ATTRIBUTE;
import static io.axual.ksml.dsl.KSMLDSL.PIPELINE_FOREACH_ATTRIBUTE;
import static io.axual.ksml.dsl.KSMLDSL.PIPELINE_TOTOPICNAMEEXTRACTOR_ATTRIBUTE;
import static io.axual.ksml.dsl.KSMLDSL.PIPELINE_TO_ATTRIBUTE;

public class PipelineSinkParser extends ContextAwareParser<StreamOperation> {
    protected PipelineSinkParser(ParseContext context) {
        super(context);
    }

    @Override
    public StreamOperation parse(YamlNode node) {
        if (node == null) return null;
        if (node.get(PIPELINE_BRANCH_ATTRIBUTE) != null) {
            return new BranchOperation(new ListParser<>(new BranchDefinitionParser(context), 1).parse(node.get(PIPELINE_BRANCH_ATTRIBUTE)));
        }
        if (node.get(PIPELINE_FOREACH_ATTRIBUTE) != null) {
            return new ForEachOperation(parseFunction(node, PIPELINE_FOREACH_ATTRIBUTE, new ForEachActionParser()));
        }
        if (node.get(PIPELINE_TOTOPICNAMEEXTRACTOR_ATTRIBUTE) != null) {
            return new ToTopicNameExtractorOperation(parseFunction(node, PIPELINE_TOTOPICNAMEEXTRACTOR_ATTRIBUTE, new TopicNameExtractorDefinitionParser()));
        }
        if (node.get(PIPELINE_TO_ATTRIBUTE) != null) {
            return new ToOperation(parseText(node, PIPELINE_TO_ATTRIBUTE));
        }
        return null;
    }
}
