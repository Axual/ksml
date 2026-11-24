package io.axual.ksml.data.notation.protobuf.apicurio;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library - Protobuf Apicurio
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
import io.axual.ksml.data.notation.Notation;
import io.axual.ksml.data.notation.NotationContext;
import io.axual.ksml.data.notation.apicurio.ApicurioConfigChecks;
import io.axual.ksml.data.notation.protobuf.ProtobufDataObjectMapper;
import io.axual.ksml.data.notation.protobuf.ProtobufNotation;
import io.axual.ksml.data.notation.vendor.VendorNotationContext;
import io.axual.ksml.data.notation.vendor.VendorNotationProvider;
import io.axual.ksml.data.util.MapUtil;
import lombok.Getter;

import java.util.Map;

public class ApicurioProtobufNotationProvider extends VendorNotationProvider {
    // Registry Client is mocked by tests
    @Getter
    private final RegistryClientFacade registryClient;

    public ApicurioProtobufNotationProvider() {
        this(null);
    }

    public ApicurioProtobufNotationProvider(RegistryClientFacade registryClient) {
        super(ProtobufNotation.NOTATION_NAME, "apicurio");
        this.registryClient = registryClient;
    }

    @Override
    public Notation createNotation(NotationContext context) {
        if (context == null) context = new NotationContext();
        ApicurioConfigChecks.rejectV2Configs(context.serdeConfigs());
        final var srClient = registryClient != null ? registryClient : createSrClient(MapUtil.stringKeys(context.serdeConfigs()));
        return new ProtobufNotation(
                new VendorNotationContext(
                        vendorName(),
                        context,
                        new ApicurioProtobufSerdeSupplier(srClient),
                        new ProtobufDataObjectMapper(new ApicurioProtobufFileElementDescriptorMapper(), context.nativeDataObjectMapper(), context.typeSchemaMapper())),
                new ApicurioProtobufSchemaParser(context.nativeDataObjectMapper(), context.typeSchemaMapper()));
    }

    // Created here rather than by the serde, so it uses the notation's serde configs and wrong settings fail at startup
    RegistryClientFacade createSrClient(Map<String, Object> serdeConfigs) {
        if (!serdeConfigs.containsKey(SchemaResolverConfig.REGISTRY_URL))
            return null;
        return RegistryClientFacadeFactory.create(new SchemaResolverConfig(serdeConfigs));
    }
}
