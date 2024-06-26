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


import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.parser.ChoiceParser;
import io.axual.ksml.parser.StructsParser;

import java.util.HashMap;
import java.util.Map;

import static io.axual.ksml.dsl.KSMLDSL.Functions;

public class TypedFunctionDefinitionParser extends ChoiceParser<FunctionDefinition> {
    public TypedFunctionDefinitionParser() {
        super(Functions.TYPE, "FunctionType", "function", Functions.TYPE_GENERIC, parsers());
    }

    private static Map<String, StructsParser<? extends FunctionDefinition>> parsers() {
        final var result = new HashMap<String, StructsParser<? extends FunctionDefinition>>();
        result.put(Functions.TYPE_AGGREGATOR, new AggregatorDefinitionParser(true));
        result.put(Functions.TYPE_FOREACHACTION, new ForEachActionDefinitionParser(true));
        result.put(Functions.TYPE_FOREIGN_KEY_EXTRACTOR, new ForeignKeyExtractorDefinitionParser(true));
        result.put(Functions.TYPE_GENERATOR, new GeneratorDefinitionParser(true));
        result.put(Functions.TYPE_GENERIC, new GenericFunctionDefinitionParser(true));
        result.put(Functions.TYPE_INITIALIZER, new InitializerDefinitionParser(true));
        result.put(Functions.TYPE_KEYTRANSFORMER, new KeyTransformerDefinitionParser(true));
        result.put(Functions.TYPE_KEYVALUETOKEYVALUELISTTRANSFORMER, new KeyValueToKeyValueListTransformerDefinitionParser(true));
        result.put(Functions.TYPE_KEYVALUETOVALUELISTTRANSFORMER, new KeyValueToValueListTransformerDefinitionParser(true));
        result.put(Functions.TYPE_KEYVALUEMAPPER, new KeyValueMapperDefinitionParser(true));
        result.put(Functions.TYPE_KEYVALUETRANSFORMER, new KeyValueTransformerDefinitionParser(true));
        result.put(Functions.TYPE_KEYVALUEPRINTER, new KeyValuePrinterDefinitionParser(true));
        result.put(Functions.TYPE_METADATATRANSFORMER, new MetadataTransformerDefinitionParser(true));
        result.put(Functions.TYPE_MERGER, new MergerDefinitionParser(true));
        result.put(Functions.TYPE_PREDICATE, new PredicateDefinitionParser(true));
        result.put(Functions.TYPE_REDUCER, new ReducerDefinitionParser(true));
        result.put(Functions.TYPE_STREAMPARTITIONER, new StreamPartitionerDefinitionParser(true));
        result.put(Functions.TYPE_TIMESTAMPEXTRACTOR, new TimestampExtractorDefinitionParser(true));
        result.put(Functions.TYPE_TOPICNAMEEXTRACTOR, new TopicNameExtractorDefinitionParser(true));
        result.put(Functions.TYPE_VALUEJOINER, new ValueJoinerDefinitionParser(true));
        result.put(Functions.TYPE_VALUETRANSFORMER, new ValueTransformerDefinitionParser(true));
        return result;
    }
}
