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
 * <p>Apicurio 3.x still uses Jackson 2 while KSML core runs on Jackson 3, and the two {@code JsonNode}
 * types are unrelated. So this mapper keeps plain native Java (Map/List/primitives) on the KSML side and
 * lets each library use its own Jackson version.</p>
 */
public class ApicurioJsonSchemaDataObjectMapper implements DataObjectMapper<Object> {
    private final NativeDataObjectMapper nativeMapper;

    public ApicurioJsonSchemaDataObjectMapper(NativeDataObjectMapper nativeMapper) {
        this.nativeMapper = nativeMapper;
    }

    /**
     * Convert a value coming out of the Apicurio deserializer into a {@link DataObject}.
     *
     * <p>The value is a Jackson 2 {@code JsonNode}, so it is turned into JSON text and parsed again
     * with Jackson 3. That text step is on purpose, not an oversight. JSON text is the only thing both
     * Jackson versions understand, and it keeps this class free of any hand-written Jackson 2 tree
     * conversion that would duplicate {@link JsonNodeUtil} and have to handle every number and binary
     * case correctly on its own.</p>
     *
     * <p>The cost is two extra JSON conversions per message, on the path every record takes. Reading
     * the Jackson 2 node directly would remove them, at the price of about 40 lines duplicating
     * {@link JsonNodeUtil}. That trade is worth revisiting only if measurement shows it matters:
     * Apicurio is expected to stay on Jackson 2 for as long as Quarkus does, so this bridge is not
     * short-lived.</p>
     *
     * @param expected the type KSML wants back, used when the value is null
     * @param value    a Jackson 2 {@code JsonNode} from Apicurio, or null for a Kafka tombstone
     * @return the value as a KSML {@link DataObject}
     */
    @Override
    public DataObject toDataObject(DataType expected, Object value) {
        if (value == null) {
            // Allow nulls (eg. Kafka tombstones), honoring the expected type.
            return ConvertUtil.convertNullToDataObject(expected);
        }
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
