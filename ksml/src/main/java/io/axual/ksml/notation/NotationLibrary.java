package io.axual.ksml.notation;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 Axual B.V.
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

import java.util.HashMap;
import java.util.Map;

import io.axual.ksml.data.parser.DataObjectParser;
import io.axual.ksml.execution.FatalError;
import io.axual.ksml.notation.avro.AvroNotation;
import io.axual.ksml.notation.binary.BinaryNotation;
import io.axual.ksml.notation.json.JsonDataObjectParser;
import io.axual.ksml.notation.json.JsonNotation;
import io.axual.ksml.notation.soap.SOAPDataObjectParser;
import io.axual.ksml.notation.soap.SOAPNotation;
import io.axual.ksml.notation.xml.XmlDataObjectParser;
import io.axual.ksml.notation.xml.XmlNotation;

public class NotationLibrary {
    private record NotationEntry(Notation notation, DataObjectParser defaultParser) {
    }

    private final Map<String, NotationEntry> notationEntries = new HashMap<>();

    public NotationLibrary(Map<String, Object> configs) {
        register(AvroNotation.NOTATION_NAME, new AvroNotation(configs), null);
        register(BinaryNotation.NOTATION_NAME, new BinaryNotation(), null);
        register(JsonNotation.NOTATION_NAME, new JsonNotation(), new JsonDataObjectParser());
        register(SOAPNotation.NOTATION_NAME, new SOAPNotation(), new SOAPDataObjectParser());
        register(XmlNotation.NOTATION_NAME, new XmlNotation(), new XmlDataObjectParser());
    }

    public void register(String name, Notation notation) {
        register(name, notation, null);
    }

    public void register(String name, Notation notation, DataObjectParser defaultParser) {
        notationEntries.put(name, new NotationEntry(notation, defaultParser));
    }

    public Notation get(String notation) {
        var result = notationEntries.get(notation);
        if (result != null) return result.notation;
        throw FatalError.dataError("Data type notation not found: " + notation);
    }

    public DataObjectParser getDefaultParser(String notation) {
        var result = notationEntries.get(notation);
        if (result != null) return result.defaultParser;
        throw FatalError.dataError("Data type notation not found: " + notation);
    }
}
