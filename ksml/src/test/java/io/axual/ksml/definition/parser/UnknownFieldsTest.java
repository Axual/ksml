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

import io.axual.ksml.data.mapper.DataObjectFlattener;
import io.axual.ksml.data.mapper.DataTypeFlattener;
import io.axual.ksml.data.notation.NotationContext;
import io.axual.ksml.data.notation.binary.BinaryNotation;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.execution.ExecutionContext;
import io.axual.ksml.parser.ParseTestSupport;
import io.axual.ksml.type.UserType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * A field that the parser does not know must stop parsing with an error that names the field, instead
 * of being silently ignored. These tests parse whole definitions through {@link TopologyDefinitionParser},
 * the same way the runner does. The definitions are in {@code pipelines/unknown-keys/}.
 */
class UnknownFieldsTest {

    private static final String DEFINITIONS = "pipelines/unknown-keys/";

    @BeforeAll
    static void registerNotations() {
        // Types like "string" use the default notation
        final var binaryNotation = new BinaryNotation(new NotationContext(new DataObjectFlattener(), new DataTypeFlattener()), null);
        ExecutionContext.INSTANCE.notationLibrary().register(UserType.DEFAULT_NOTATION, binaryNotation);
        ExecutionContext.INSTANCE.notationLibrary().register(BinaryNotation.NOTATION_NAME, binaryNotation);
    }

    // The messages of an exception and all its causes
    private static String causeMessages(Throwable thrown) {
        final var result = new StringBuilder();
        for (var cause = thrown; cause != null; cause = cause.getCause()) {
            result.append(cause.getMessage()).append('\n');
        }
        return result.toString();
    }

    private static void parse(String file) throws Exception {
        final var path = Path.of(ClassLoader.getSystemResource(DEFINITIONS + file).toURI());
        new TopologyDefinitionParser("test").parse(ParseTestSupport.nodeOf(Files.readString(path)));
    }

    @ParameterizedTest(name = "{0} parses")
    @ValueSource(strings = {"valid-definition.yaml", "name-version-description.yaml"})
    @DisplayName("A valid definition parses")
    void validDefinitionParses(String file) {
        assertThatCode(() -> parse(file)).doesNotThrowAnyException();
    }

    @ParameterizedTest(name = "{0} is rejected, naming {1}")
    @CsvSource({
            "store-misspelled-option.yaml,         loging",
            "store-two-unknown-fields.yaml,        persistant;loging",
            "window-by-time-misspelled-grace.yaml, grase",
            "top-level-misspelled-section.yaml,    pipeline",
            "filter-field-of-other-operation.yaml, duration",
            "inline-topic-unknown-field.yaml,      valuetype",
            "inline-function-unknown-field.yaml,   resultTyp",
            "print-unknown-field.yaml,             labl"
    })
    @DisplayName("An unknown field is rejected and named in the error")
    void unknownFieldIsRejected(String file, String unknownFields) {
        final var thrown = assertThatThrownBy(() -> parse(file)).hasStackTraceContaining("Unknown field");
        for (final var field : unknownFields.split(";")) {
            thrown.hasStackTraceContaining("\"" + field + "\"");
        }
    }

    @Test
    @DisplayName("An invalid operation type is reported as such, not as unknown fields")
    void invalidOperationTypeKeepsItsError() {
        final var thrown = catchThrowable(() -> parse("invalid-operation-type.yaml"));

        assertThat(causeMessages(thrown))
                .contains("Unknown operation")
                .doesNotContain("Unknown field");
    }

    @Test
    @DisplayName("A branch inside a branch is reported as not supported")
    void nestedBranchIsNotSupported() {
        assertThatThrownBy(() -> parse("nested-branch.yaml"))
                .hasStackTraceContaining("A branch inside a branch is not supported yet");
    }

    @Test
    @DisplayName("The schema accepts the top-level name, version and description")
    void schemaHasTopLevelNameVersionDescription() {
        final var schema = (StructSchema) new TopologyDefinitionParser("test").schema();

        assertThat(schema.fields())
                .extracting(StructSchema.Field::name)
                .contains("name", "version", "description");
    }
}
