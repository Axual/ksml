package io.axual.ksml.data.notation.avro.apicurio;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library - AVRO Apicurio
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

import io.apicurio.registry.resolver.client.RegistryClientFacade;
import io.apicurio.registry.resolver.client.RegistryClientFacadeFactory;
import io.apicurio.registry.resolver.config.SchemaResolverConfig;
import io.axual.ksml.client.resolving.ResolvingClientConfig;
import io.axual.ksml.data.notation.Notation;
import io.axual.ksml.data.notation.NotationContext;
import io.axual.ksml.data.notation.avro.AvroDataObjectMapper;
import io.axual.ksml.data.notation.avro.AvroNotation;
import io.axual.ksml.data.notation.vendor.VendorNotationContext;
import io.axual.ksml.data.notation.vendor.VendorNotationProvider;
import io.axual.ksml.data.util.MapUtil;

import java.util.HashMap;
import java.util.Map;

public class ApicurioAvroNotationProvider extends VendorNotationProvider {
    private final RegistryClientFacade registryClient;

    public ApicurioAvroNotationProvider() {
        this(null);
    }

    public ApicurioAvroNotationProvider(RegistryClientFacade registryClient) {
        super(AvroNotation.NOTATION_NAME, "apicurio");
        this.registryClient = registryClient;
    }

    @Override
    public Notation createNotation(NotationContext context) {
        final Map<String, Object> serdeConfigs = context != null ? MapUtil.stringKeys(context.serdeConfigs()) : new HashMap<>();
        final var clientConfig = new ResolvingClientConfig(serdeConfigs);
        final var srClient = this.registryClient != null ? registryClient : createSrClient(serdeConfigs);
        return new ApicurioAvroNotation(
                new VendorNotationContext(vendorName(), context, new ApicurioAvroSerdeSupplier(srClient), new AvroDataObjectMapper()),
                srClient,
                clientConfig.topicResolver());
    }

    private RegistryClientFacade createSrClient(Map<String, Object> serdeConfigs) {
        if (!serdeConfigs.containsKey(SchemaResolverConfig.REGISTRY_URL))
            return null;
        return RegistryClientFacadeFactory.create(new SchemaResolverConfig(serdeConfigs));
    }
}
