package io.axual.ksml.data.notation.jsonschema.apicurio;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library - JSON Schema Apicurio
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

import io.apicurio.registry.rest.client.RegistryClient;
import io.axual.ksml.data.notation.Notation;
import io.axual.ksml.data.notation.NotationContext;
import io.axual.ksml.data.notation.jsonschema.JsonSchemaDataObjectMapper;
import io.axual.ksml.data.notation.jsonschema.JsonSchemaNotation;
import io.axual.ksml.data.notation.vendor.VendorNotationContext;
import io.axual.ksml.data.notation.vendor.VendorNotationProvider;

/**
 * NotationProvider for JSON Schema using the Apicurio Registry vendor.
 *
 * <p>Exposes notationName = "jsonschema" and vendorName = "apicurio" via
 * the {@link VendorNotationProvider} base class.</p>
 *
 * <p>When asked to create a notation, this provider wires a {@link JsonSchemaNotation}
 * with an {@link ApicurioJsonSchemaSerdeSupplier} and a {@link JsonSchemaDataObjectMapper}
 * using the {@link io.axual.ksml.data.mapper.NativeDataObjectMapper} from the provided
 * {@link NotationContext}.</p>
 */
public class ApicurioJsonSchemaNotationProvider extends VendorNotationProvider {
    private final RegistryClient registryClient;

    public ApicurioJsonSchemaNotationProvider() {
        this(null);
    }

    public ApicurioJsonSchemaNotationProvider(RegistryClient registryClient) {
        super(JsonSchemaNotation.NOTATION_NAME, "apicurio");
        this.registryClient = registryClient;
    }

    @Override
    public Notation createNotation(NotationContext context) {
        if (context == null) context = new NotationContext();
        return new JsonSchemaNotation(
                new VendorNotationContext(
                        vendorName(),
                        context,
                        new ApicurioJsonSchemaSerdeSupplier(registryClient),
                        new JsonSchemaDataObjectMapper(context.nativeDataObjectMapper())));
    }
}
