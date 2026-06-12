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

import com.ctc.wstx.stax.WstxInputFactory;
import com.ctc.wstx.stax.WstxOutputFactory;
import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.notation.string.StringMapper;
import io.axual.ksml.data.util.JsonNodeUtil;
import tools.jackson.databind.SerializationFeature;
import tools.jackson.dataformat.xml.XmlFactory;
import tools.jackson.dataformat.xml.XmlMapper;
import tools.jackson.dataformat.xml.XmlWriteFeature;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import java.io.IOException;
import java.io.StringWriter;

public class XmlStringMapper implements StringMapper<Object> {
    private final XmlMapper mapper;
    private final String rootName;
    private final boolean prettyPrint;

    public XmlStringMapper(String rootName, boolean prettyPrint) {
        final var inputFactory = new WstxInputFactory();
        inputFactory.setProperty(XMLInputFactory.IS_NAMESPACE_AWARE, Boolean.FALSE);
        final var outputFactory = new WstxOutputFactory();
        outputFactory.setProperty(XMLOutputFactory.IS_REPAIRING_NAMESPACES, Boolean.TRUE);
        var builder = XmlMapper.builder(new XmlFactory(inputFactory, outputFactory)).enable(XmlWriteFeature.WRITE_XML_1_1);
        if (prettyPrint) builder = builder.enable(SerializationFeature.INDENT_OUTPUT);
        mapper = builder.build();
        this.rootName = rootName;
        this.prettyPrint = prettyPrint;
    }

    @Override
    public Object fromString(String value) {
        if (value == null) return null; // Allow null strings as input, returning null as native output
        try {
            var tree = mapper.readTree(value);
            return JsonNodeUtil.convertJsonNodeToNative(tree);
        } catch (Exception _) {
            throw new DataException("Could not parse string to object: " + value);
        }
    }

    @Override
    public String toString(Object value) {
        if (value == null) return null; // Allow null as native input, return null string as output
        try (var stringWriter = new StringWriter()) {
            final var objectWriter = prettyPrint ? mapper.writerWithDefaultPrettyPrinter() : mapper.writer();
            objectWriter
                    .withRootName(rootName)
                    .without(SerializationFeature.INDENT_OUTPUT)
                    .writeValue(stringWriter, value);
            return stringWriter.toString();
        } catch (IOException e) {
            throw new DataException("Can not convert object to JSON string: " + value, e);
        }
    }
}
