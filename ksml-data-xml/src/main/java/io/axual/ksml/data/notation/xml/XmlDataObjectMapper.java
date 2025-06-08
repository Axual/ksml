package io.axual.ksml.data.notation.xml;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library - XML
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

import io.axual.ksml.data.mapper.DataObjectMapper;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.type.DataType;

public class XmlDataObjectMapper implements DataObjectMapper<String> {
    private static final NativeDataObjectMapper NATIVE_MAPPER = new NativeDataObjectMapper();
    private final boolean prettyPrint;

    public XmlDataObjectMapper(boolean prettyPrint) {
        this.prettyPrint = prettyPrint;
    }

    @Override
    public DataObject toDataObject(DataType expected, String value) {
        final var stringMapper = new XmlStringMapper("dummy", prettyPrint);
        final var object = stringMapper.fromString(value);
        return NATIVE_MAPPER.toDataObject(expected, object);
    }

    @Override
    public String fromDataObject(DataObject value) {
        final var object = NATIVE_MAPPER.fromDataObject(value);
        final var stringMapper = new XmlStringMapper(value.type().name(), prettyPrint);
        return stringMapper.toString(object);
    }
}
