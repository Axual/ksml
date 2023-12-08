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


import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.operation.StreamOperation;
import io.axual.ksml.parser.YamlNode;

import static io.axual.ksml.dsl.KSMLDSL.*;

public class PipelineSinkOperationParser extends OperationParser<StreamOperation> {
    public PipelineSinkOperationParser(String prefix, String name, TopologyResources resources) {
        super(prefix, name, resources);
    }

    @Override
    public StreamOperation parse(YamlNode node) {
        if (node == null) return null;
        if (node.get(Operations.AS) != null) {
            // Parser can parse references, so pass in the node itself rather than the child
            return new AsOperationParser(prefix, determineName("as"), resources).parse(node);
        }
        if (node.get(Operations.BRANCH) != null) {
            return new BranchOperationParser(prefix, determineName("branch"), resources).parse(node.get(Operations.BRANCH));
        }
        if (node.get(Operations.FOR_EACH) != null) {
            // Parser can parse references, so pass in the node itself rather than the child
            return new ForEachOperationParser(prefix, determineName("for_each"), resources).parse(node);
        }
        if (node.get(Operations.PRINT) != null) {
            return new PrintOperationParser(prefix, determineName("print"), resources).parse(node.get(Operations.PRINT));
        }
        if (node.get(Operations.TO) != null) {
            // Parser can parse references, so pass in the node itself rather than the child
            return new ToOperationParser(prefix, determineName("to"), resources).parse(node);
        }
        return null;
    }
}
