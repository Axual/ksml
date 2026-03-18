package io.axual.ksml.python;

/*-
 * ========================LICENSE_START=================================
 * KSML
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

import io.axual.ksml.data.util.ValuePrinter;
import org.graalvm.polyglot.Value;

public class PythonValuePrinter extends ValuePrinter {
    private static final String SINGLE_QUOTE = "'";
    private static final String PYTHON_NULL = "None";
    private static final String PYTHON_TRUE = "True";
    private static final String PYTHON_FALSE = "False";

    public PythonValuePrinter() {
        super(new ValuePrinterDict(SINGLE_QUOTE, PYTHON_NULL, PYTHON_TRUE, PYTHON_FALSE));
    }

    @Override
    public String print(Object value, boolean quoted) {
        return super.print(unwrap(value), quoted);
    }

    private Object unwrap(Object value) {
        if (!(value instanceof Value val)) return value;
        if (val.isNull()) return null;
        if (val.isString()) return val.asString();
        if (val.isBoolean()) return val.asBoolean();
        if (val.isNumber()) {
            if (val.fitsInInt()) return val.asInt();
            if (val.fitsInLong()) return val.asLong();
            return val.asDouble();
        }
        return val;
    }
}
