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

import io.axual.ksml.execution.FatalError;
import io.axual.ksml.notation.avro.AvroNotation;
import io.axual.ksml.notation.binary.BinaryNotation;
import io.axual.ksml.notation.json.JsonNotation;
import io.axual.ksml.notation.soap.SOAPNotation;
import io.axual.ksml.notation.xml.XmlNotation;

public class NotationLibrary {
    private final Map<String, Notation> notations = new HashMap<>();

    public NotationLibrary(Map<String, Object> configs) {
        register(AvroNotation.NOTATION_NAME, new AvroNotation(configs));
        register(BinaryNotation.NOTATION_NAME, new BinaryNotation());
        register(JsonNotation.NOTATION_NAME, new JsonNotation());
        register(SOAPNotation.NOTATION_NAME, new SOAPNotation());
        register(XmlNotation.NOTATION_NAME, new XmlNotation());
    }

    public void register(String name, Notation notation) {
        notations.put(name, notation);
    }

    public Notation get(String notation) {
        var result = notations.get(notation);
        if (result != null) return result;
        throw FatalError.dataError("Data type notation not found: " + notation);
    }
}
