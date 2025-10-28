package io.axual.ksml.runner.config.internal;

/*-
 * ========================LICENSE_START=================================
 * KSML Runner
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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

// Serializer
public class KsmlFileOrDefinitionSerializer extends StdSerializer<KsmlFileOrDefinition> {
    public KsmlFileOrDefinitionSerializer() {
        super(KsmlFileOrDefinition.class);
    }

    @Override
    public void serialize(KsmlFileOrDefinition value, JsonGenerator gen, SerializerProvider provider) throws IOException {

        if (value instanceof KsmlFilePath sv) {
            gen.writeString(sv.getValue());
        } else if (value instanceof KsmlInlineDefinition ov) {
            provider.defaultSerializeValue(ov.getValue(), gen);
        }
    }
}
