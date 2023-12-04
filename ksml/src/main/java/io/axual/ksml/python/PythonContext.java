package io.axual.ksml.python;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2023 Axual B.V.
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

import io.axual.ksml.data.mapper.DataObjectConverter;
import io.axual.ksml.execution.FatalError;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.PolyglotAccess;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.io.IOAccess;

@Slf4j
public class PythonContext {
    private static final String PYTHON = "python";
    private final Context context;
    @Getter
    private final DataObjectConverter converter;

    public PythonContext() {
        this.converter = new DataObjectConverter();

        log.debug("Setting up new Python context");
        try {
            context = Context.newBuilder(PYTHON)
                    .allowIO(IOAccess.ALL)
                    .allowNativeAccess(true)
                    .allowPolyglotAccess(PolyglotAccess.ALL)
                    .allowHostAccess(HostAccess.ALL)
                    .allowHostClassLookup(name -> name.equals("java.util.ArrayList") || name.equals("java.util.HashMap") || name.equals("java.util.TreeMap"))
                    .build();
        } catch (Exception e) {
            log.error("Error setting up a new Python context", e);
            throw FatalError.executionError("Could not setup a new Python context", e);
        }
    }

    public Value registerFunction(String pyCode, String callerName) {
        Source script = Source.create(PYTHON, pyCode);
        try {
            context.eval(script);
        } catch (Exception e) {
            log.error("Error loading Python code", e);
        }
        return context.getPolyglotBindings().getMember(callerName);
    }
}
