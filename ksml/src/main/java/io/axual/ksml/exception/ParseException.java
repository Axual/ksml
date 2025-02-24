package io.axual.ksml.exception;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library
 * %%
 * Copyright (C) 2021 - 2024 Axual B.V.
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

import io.axual.ksml.data.exception.BaseException;
import io.axual.ksml.parser.ParseNode;

public class ParseException extends BaseException {
    private static final String ACTIVITY = "parse";

    public ParseException(ParseNode node, String message) {
        this(node, message, null);
    }

    public ParseException(String message) {
        super(ACTIVITY, message);
    }

    public ParseException(ParseNode node, String message, Throwable cause) {
        super(ACTIVITY, "Error in input node " + (node != null ? node.toString() : "<root>") + ": " + message, cause);
    }
}
