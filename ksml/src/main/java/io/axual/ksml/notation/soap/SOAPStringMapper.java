package io.axual.ksml.notation.soap;

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

import io.axual.ksml.execution.FatalError;
import io.axual.ksml.notation.string.StringMapper;
import jakarta.xml.soap.MessageFactory;
import jakarta.xml.soap.SOAPConstants;
import jakarta.xml.soap.SOAPException;
import jakarta.xml.soap.SOAPMessage;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class SOAPStringMapper implements StringMapper<SOAPMessage> {
    private final MessageFactory messageFactory;

    public SOAPStringMapper() {
        try {
            messageFactory = MessageFactory.newInstance(SOAPConstants.SOAP_1_2_PROTOCOL);
        } catch (SOAPException e) {
            throw FatalError.dataError("Could not create SOAP Message Factory", e);
        }
    }

    @Override
    public SOAPMessage fromString(String value) {
        if (value.startsWith("<?xml")) {
            value = value.substring(value.indexOf("?>") + 2);
        }
        var reader = new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8));
        try {
            return messageFactory.createMessage(null, reader);
        } catch (SOAPException | IOException e) {
            throw FatalError.dataError("Could not convert String to SOAPMessage", e);
        }
    }

    @Override
    public String toString(SOAPMessage value) {
        var writer = new ByteArrayOutputStream();
        try {
            value.writeTo(writer);
            return writer.toString(StandardCharsets.UTF_8);
        } catch (SOAPException | IOException e) {
            throw FatalError.dataError("Could not convert SOAPMessage to String", e);
        }
    }
}
