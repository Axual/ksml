package io.axual.ksml.data.notation.string;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2023 Axual B.V.
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

import io.axual.ksml.data.mapper.DataObjectMapper;
import io.axual.ksml.data.notation.NotationContext;
import io.axual.ksml.data.notation.base.BaseNotation;
import io.axual.ksml.data.serde.StringSerde;
import io.axual.ksml.data.type.DataType;
import org.apache.kafka.common.serialization.Serde;

/**
 * Base notation implementation for notations that internally serialize to textual String form.
 */
public abstract class StringNotation extends BaseNotation {

    protected StringNotation(NotationContext context) {
        super(context);
    }

    /**
     * Returns the mapper used to convert DataObjects to/from their String representation.
     *
     * @return the string mapper for this notation
     */
    protected abstract DataObjectMapper<String> stringMapper();

    /**
     * Creates a StringSerde configured for the requested data type and key/value role.
     *
     * @param type  the data type to serialize/deserialize
     * @param isKey whether the serde will be used for keys (true) or values (false)
     * @return a configured String-backed Serde
     */
    @Override
    public Serde<Object> serde(DataType type, boolean isKey) {
        final var result = new StringSerde(context().nativeDataObjectMapper(), stringMapper(), type);
        result.configure(context().serdeConfigs(), isKey);
        return result;
    }
}
