package io.axual.ksml.data.notation.jsonschema.apicurio;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library - JSON Schema Apicurio
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

import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.mapper.DataObjectMapper;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.util.ConvertUtil;
import io.axual.ksml.data.util.JsonNodeUtil;

/**
 * Maps between KSML {@link DataObject}s and the JSON values exchanged with Apicurio's JSON Schema serde.
 *
 * <p>Apicurio 3.x still (de)serializes JSON Schema payloads with Jackson 2, while KSML core runs on
 * Jackson 3. A Jackson 2 {@code JsonNode} is not assignable to Jackson 3's {@code JsonNode}, so this
 * mapper keeps plain native Java (Map/List/primitives) on the KSML side:</p>
 * <ul>
 *   <li>serialization hands Apicurio a native value, which its own Jackson 2 mapper encodes as
 *       schema-valid JSON (a Jackson 3 node would instead be bean-serialized into garbage);</li>
 *   <li>deserialization receives a Jackson 2 {@code JsonNode}, bridged to native via its JSON string
 *       form, which both Jackson major versions render identically.</li>
 * </ul>
 */
public class ApicurioJsonSchemaDataObjectMapper implements DataObjectMapper<Object> {
    private final NativeDataObjectMapper nativeMapper;

    public ApicurioJsonSchemaDataObjectMapper(NativeDataObjectMapper nativeMapper) {
        this.nativeMapper = nativeMapper;
    }

    @Override
    public DataObject toDataObject(DataType expected, Object value) {
        if (value == null) {
            // Allow nulls (eg. Kafka tombstones), honoring the expected type.
            return ConvertUtil.convertNullToDataObject(expected);
        }
        // Apicurio returns a Jackson 2 JsonNode; its toString() is valid JSON that KSML's Jackson 3 parser reads.
        final var tree = JsonNodeUtil.convertStringToJsonNode(value.toString());
        if (tree == null) throw new DataException("Cannot convert value to DataObject: " + value);
        return nativeMapper.toDataObject(expected, JsonNodeUtil.convertJsonNodeToNative(tree));
    }

    @Override
    public Object fromDataObject(DataObject value) {
        // Return native Java; Apicurio's Jackson 2 serializer encodes it, avoiding a cross-version JsonNode.
        return nativeMapper.fromDataObject(value);
    }
}
