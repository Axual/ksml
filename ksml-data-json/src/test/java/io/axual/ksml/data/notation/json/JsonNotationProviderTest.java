package io.axual.ksml.data.notation.json;

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

import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.axual.ksml.data.notation.Notation;
import io.axual.ksml.data.notation.NotationContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link JsonNotationProvider}, ensuring it exposes the correct
 * notation name and creates a {@link JsonNotation} configured with the supplied
 * {@link NotationContext}. Tests follow the AssertJ chained style.
 */
@DisplayName("JsonNotationProvider - notation name and factory behavior")
class JsonNotationProviderTest {

    private final JsonNotationProvider provider = new JsonNotationProvider();

    @Test
    @DisplayName("notationName() returns 'json'")
    void notationNameIsJson() {
        assertThat(provider.notationName()).isEqualTo(JsonNotation.NOTATION_NAME);
    }

    @Test
    @DisplayName("createNotation(context) returns JsonNotation with correct wiring")
    void createNotationBuildsJsonNotation() {
        // Given a context without vendor
        var context = new NotationContext(JsonNotation.NOTATION_NAME);

        // When
        Notation notation = provider.createNotation(context);

        // Then: verify type and key properties via chained assertions
        assertThat(notation)
                .asInstanceOf(InstanceOfAssertFactories.type(JsonNotation.class))
                .returns("json", n -> n.name())
                .returns(".json", n -> n.filenameExtension())
                .returns(JsonNotation.DEFAULT_TYPE, n -> n.defaultType());

        // And: verify converter and schema parser are of expected JSON-specific types
        var softly = new SoftAssertions();
        softly.assertThat(notation.converter()).isInstanceOf(JsonDataObjectConverter.class);
        softly.assertThat(notation.schemaParser()).isInstanceOf(JsonSchemaLoader.class);
        softly.assertAll();
    }

    @Test
    @DisplayName("Vendor in context prefixes created notation name")
    void nameIncludesVendorFromContext() {
        var context = new NotationContext(JsonNotation.NOTATION_NAME, "vendorX");
        var notation = provider.createNotation(context);
        assertThat(notation.name()).isEqualTo("vendorX_json");
    }
}
