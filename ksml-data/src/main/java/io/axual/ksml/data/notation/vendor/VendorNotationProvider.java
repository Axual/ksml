package io.axual.ksml.data.notation.vendor;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library
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

import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.notation.NotationProvider;
import lombok.Getter;

import java.util.Map;

/**
 * Convenience base for vendor-specific {@link NotationProvider} implementations.
 *
 * <p>Captures the generic notation name (e.g., "avro", "jsonschema") and the concrete vendor
 * identity (e.g., "apicurio", "confluent"). Subclasses can use these for naming, logging and
 * discovery.</p>
 */
@Getter
public abstract class VendorNotationProvider implements NotationProvider {
    /** Generic notation name (e.g., "avro"). */
    private final String notationName;
    /** Concrete vendor identity (e.g., "apicurio"). */
    private final String vendorName;

    /**
     * Create a vendor-specific notation provider.
     *
     * @param notationName generic notation identifier
     * @param vendorName   vendor identity
     */
    protected VendorNotationProvider(String notationName, String vendorName) {
        this.notationName = notationName;
        this.vendorName = vendorName;
    }

    /**
     * Fail fast when a configuration key that a vendor has renamed is still present. Passing the config
     * straight through would otherwise silently ignore the old key (e.g. dropped credentials), so we
     * reject it with a message pointing at the replacement key instead of letting the failure surface
     * later as an unexplained authentication or lookup error.
     *
     * @param configs        the serde configuration to inspect (nullable)
     * @param deprecatedKey  the key that is no longer read by the vendor
     * @param replacementKey the key that should be used instead
     */
    protected static void rejectRenamedConfigKey(Map<String, ?> configs, String deprecatedKey, String replacementKey) {
        if (configs != null && configs.containsKey(deprecatedKey))
            throw new DataException("Configuration key '" + deprecatedKey + "' is no longer supported; use '" + replacementKey + "' instead");
    }
}
