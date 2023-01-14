package io.axual.ksml.notation.xml;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2022 Axual B.V.
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

import io.axual.ksml.execution.FatalError;
import io.axual.ksml.notation.string.CustomStringMapper;

public class XmlStringMapper extends CustomStringMapper {
    private static final ObjectMapper MAPPER = new XmlMapper();

    public XmlStringMapper() {
        super(MAPPER);
    }

    @Override
    public Object fromString(String value) {
        // If there is a header, remove it first before parsing
        if (value.startsWith("<?xml")) {
            value = value.substring(value.indexOf("?>") + 2);
        }
        // Add a dummy tag before and after to make Jackson return the root element as part of
        // the resulting object
        return super.fromString("<dummyRootTag>" + value + "</dummyRootTag>");
    }

    @Override
    public String toString(Object value) {
        try {
            return mapper.writer().withRootName("object").writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw FatalError.dataError("Can not convert object to XML string: " + (value != null ? value.toString() : "null"), e);
        }
    }
}
