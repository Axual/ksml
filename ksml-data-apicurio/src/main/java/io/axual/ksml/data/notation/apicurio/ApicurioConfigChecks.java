package io.axual.ksml.data.notation.apicurio;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library - Apicurio common
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

import io.apicurio.registry.resolver.config.SchemaResolverConfig;
import io.apicurio.registry.serde.Default4ByteIdHandler;
import io.apicurio.registry.serde.config.SerdeConfig;
import io.axual.ksml.data.exception.DataException;

import java.util.Map;

/**
 * Startup checks shared by the Apicurio-backed notations.
 *
 * <p>Apicurio v3 renamed or removed several v2 settings. Passing such a setting through would drop it
 * without a word, so the Avro, JSON Schema and Protobuf providers all reject it the same way. Keeping the
 * rules here means a new one is added once instead of three times.</p>
 */
public final class ApicurioConfigChecks {
    /** Apicurio v2 id handler, removed in v3. Kept as a literal because the class is gone. */
    public static final String LEGACY_4_BYTE_ID_HANDLER = "io.apicurio.registry.serde.Legacy4ByteIdHandler";

    private static final String V2_AUTH_USERNAME = "apicurio.auth.username";
    private static final String V2_AUTH_PASSWORD = "apicurio.auth.password";
    private static final String V2_AS_CONFLUENT = "apicurio.registry.as-confluent";

    private ApicurioConfigChecks() {
    }

    /**
     * Reject every Apicurio v2 setting that v3 renamed or removed.
     *
     * @param configs the serde configuration to inspect (nullable)
     * @throws DataException if a v2 setting is still present
     */
    public static void rejectV2Configs(Map<String, ?> configs) {
        if (configs == null) return;
        rejectRenamedKey(configs, V2_AUTH_USERNAME, SchemaResolverConfig.AUTH_USERNAME);
        rejectRenamedKey(configs, V2_AUTH_PASSWORD, SchemaResolverConfig.AUTH_PASSWORD);
        rejectRemovedKey(configs, V2_AS_CONFLUENT,
                "the payload id format now follows " + SerdeConfig.ID_HANDLER + " and " + SerdeConfig.USE_ID);
        rejectRemovedValue(configs, SerdeConfig.ID_HANDLER,
                LEGACY_4_BYTE_ID_HANDLER, Default4ByteIdHandler.class.getCanonicalName());
    }

    // A renamed key would be ignored, so dropped credentials would only surface later as a 401.
    private static void rejectRenamedKey(Map<String, ?> configs, String deprecatedKey, String replacementKey) {
        if (configs.containsKey(deprecatedKey))
            throw new DataException("Configuration key '" + deprecatedKey + "' is no longer supported; use '" + replacementKey + "' instead");
    }

    // A removed key has no one-to-one replacement, so the caller supplies the advice to show.
    private static void rejectRemovedKey(Map<String, ?> configs, String removedKey, String advice) {
        if (configs.containsKey(removedKey))
            throw new DataException("Configuration key '" + removedKey + "' no longer exists; " + advice);
    }

    // The key is still valid but the value is not: putIfAbsent keeps it, so the serde would only fail
    // later when it tries to load a class that is gone.
    private static void rejectRemovedValue(Map<String, ?> configs, String key, String removedValue, String replacementValue) {
        final var value = configs.get(key);
        if (value != null && removedValue.equals(String.valueOf(value)))
            throw new DataException("Configuration key '" + key + "' is set to '" + removedValue
                    + "', which no longer exists; use '" + replacementValue + "' instead");
    }
}
