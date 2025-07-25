package io.axual.ksml.data.notation.jsonschema;

/*-
 * ========================LICENSE_START=================================
 * KSML
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

import com.fasterxml.jackson.databind.JsonNode;
import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.mapper.DataObjectMapper;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.notation.vendor.VendorNotation;
import io.axual.ksml.data.notation.json.JsonDataObjectConverter;
import io.axual.ksml.data.notation.json.JsonSchemaLoader;
import io.axual.ksml.data.notation.vendor.VendorNotationContext;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.data.type.StructType;
import io.axual.ksml.data.type.UnionType;
import io.axual.ksml.data.util.JsonNodeUtil;

import java.util.Map;

public class JsonSchemaNotation extends VendorNotation {
    public static final String NOTATION_NAME = "jsonschema";
    public static final DataType DEFAULT_TYPE = new UnionType(
            new UnionType.MemberType(new StructType()),
            new UnionType.MemberType(new ListType()));

    public JsonSchemaNotation(VendorNotationContext context) {
        super(context,  ".json", DEFAULT_TYPE, new JsonDataObjectConverter(), new JsonSchemaLoader());
    }
}
