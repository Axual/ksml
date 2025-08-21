package io.axual.ksml.data.serde;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library
 * %%
 * Copyright (C) 2021 - 2024 Axual B.V.
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

import io.axual.ksml.data.type.DataType;
import org.apache.kafka.common.serialization.Serde;

/**
 * A strategy interface to obtain a Kafka Serde for a specific KSML DataType and key/value role.
 * Implementations typically choose appropriate underlying Serdes based on the data model.
 */
public interface SerdeSupplier {
    /**
     * Returns a Serde capable of handling the given KSML DataType.
     *
     * @param type  the KSML data type to support
     * @param isKey whether the Serde will be used for keys (true) or values (false)
     * @return a Serde<Object> suitable for the given type and role
     */
    Serde<Object> get(DataType type, boolean isKey);
}
