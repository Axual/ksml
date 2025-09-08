package io.axual.ksml.data.notation.jsonschema.confluent;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library - JSON Schema Confluent
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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.axual.ksml.data.notation.NotationContext;
import io.axual.ksml.data.notation.jsonschema.JsonSchemaNotation;
import io.axual.ksml.data.notation.vendor.VendorNotation;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link ConfluentJsonSchemaNotationProvider} ensuring provider metadata and
 * notation wiring behave as expected. Mirrors patterns used in ConfluentAvroNotationProviderTest.
 */
@DisplayName("ConfluentJsonSchemaNotationProvider - metadata and wiring")
class ConfluentJsonSchemaNotationProviderTest {

    @Test
    @DisplayName("Provider exposes notation/vendor names for Confluent JSON Schema")
    void providerMetadata_isCorrect() {
        assertThat(new ConfluentJsonSchemaNotationProvider())
                .returns(JsonSchemaNotation.NOTATION_NAME, ConfluentJsonSchemaNotationProvider::notationName)
                .returns("confluent", ConfluentJsonSchemaNotationProvider::vendorName);
    }

    @Test
    @DisplayName("createNotation wires JsonSchemaNotation with Confluent serde supplier")
    void createNotation_wiresConfluentSerdeSupplier() {
        var provider = new ConfluentJsonSchemaNotationProvider();
        var context = new NotationContext(JsonSchemaNotation.NOTATION_NAME, "confluent");

        assertThat(provider.createNotation(context))
                .asInstanceOf(InstanceOfAssertFactories.type(JsonSchemaNotation.class))
                .returns("confluent_" + JsonSchemaNotation.NOTATION_NAME, JsonSchemaNotation::name)
                .returns(".json", JsonSchemaNotation::filenameExtension)
                .returns(JsonSchemaNotation.DEFAULT_TYPE, JsonSchemaNotation::defaultType)
                .asInstanceOf(InstanceOfAssertFactories.type(VendorNotation.class))
                .extracting(VendorNotation::serdeSupplier)
                .asInstanceOf(InstanceOfAssertFactories.type(ConfluentJsonSchemaSerdeSupplier.class));
    }
}
