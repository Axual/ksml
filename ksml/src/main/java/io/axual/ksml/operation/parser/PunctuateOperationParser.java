package io.axual.ksml.operation.parser;

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

import io.axual.ksml.data.schema.EnumSchema;
import io.axual.ksml.definition.parser.PunctuatorDefinitionParser;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.exception.TopologyException;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.operation.PunctuateOperation;
import io.axual.ksml.parser.FieldParsers;
import io.axual.ksml.parser.StringValueParser;
import io.axual.ksml.parser.StructsParser;
import org.apache.kafka.streams.processor.PunctuationType;

import java.util.List;

import static io.axual.ksml.dsl.KSMLDSL.Operations.Punctuate;

public class PunctuateOperationParser extends OperationParser<PunctuateOperation> {
    private static final EnumSchema PUNCTUATION_TYPE_SCHEMA = new EnumSchema(
            FieldParsers.SCHEMA_NAMESPACE,
            "PunctuationType",
            "The time that schedules the punctuator: wallClockTime (system time, also without input) or streamTime (record timestamps)",
            List.of(new EnumSchema.Symbol(Punctuate.PUNCTUATION_TYPE_WALL_CLOCK_TIME), new EnumSchema.Symbol(Punctuate.PUNCTUATION_TYPE_STREAM_TIME)));

    public PunctuateOperationParser(TopologyResources resources) {
        super(KSMLDSL.Operations.PUNCTUATE, resources);
    }

    @Override
    protected StructsParser<PunctuateOperation> parser() {
        return FieldParsers.structsParser(
                PunctuateOperation.class,
                "",
                "Run a punctuator on a schedule, which can forward records to the next operation; records from the stream pass unchanged",
                operationNameField(),
                FieldParsers.durationField(Punctuate.INTERVAL, "The time between two punctuations, at least 1 millisecond"),
                FieldParsers.enumField(Punctuate.PUNCTUATION_TYPE, PUNCTUATION_TYPE_SCHEMA),
                functionField(Punctuate.PUNCTUATOR, "The function that is called on each punctuation", new PunctuatorDefinitionParser(false)),
                FieldParsers.optional(FieldParsers.listField(Punctuate.STORES, "store-name", "store", "The names of the state stores the punctuator uses, in addition to those of the function", new StringValueParser())),
                (name, interval, punctuationType, punctuator, stores, tags) -> {
                    requireField(interval, Punctuate.INTERVAL);
                    requireField(punctuationType, Punctuate.PUNCTUATION_TYPE);
                    requireField(punctuator, Punctuate.PUNCTUATOR);
                    if (!KSMLDSL.Functions.TYPE_PUNCTUATOR.equals(punctuator.type()) && !KSMLDSL.Functions.TYPE_GENERIC.equals(punctuator.type()))
                        throw new TopologyException("Punctuate operation needs a function of type " + KSMLDSL.Functions.TYPE_PUNCTUATOR + ", but function '" + punctuator.name() + "' has type " + punctuator.type());
                    if (interval.toMillis() < 1)
                        throw new TopologyException("Punctuate interval must be at least 1 millisecond, got " + interval);
                    final var type = Punctuate.PUNCTUATION_TYPE_STREAM_TIME.equals(punctuationType)
                            ? PunctuationType.STREAM_TIME
                            : PunctuationType.WALL_CLOCK_TIME;
                    return new PunctuateOperation(operationConfig(name, tags), interval, type, punctuator, stores);
                });
    }

    private static void requireField(Object value, String field) {
        if (value == null) throw new TopologyException("Punctuate operation needs the field '" + field + "'");
    }
}
