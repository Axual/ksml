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

import io.axual.ksml.data.exception.ExecutionException;
import io.axual.ksml.data.mapper.DataObjectConverter;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.graalvm.polyglot.*;
import org.graalvm.polyglot.io.IOAccess;

@Slf4j
public class PythonContext {
    private static final LoggerBridge LOGGER_BRIDGE = new LoggerBridge();
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
            registerGlobalCode();
        } catch (Exception e) {
            log.error("Error setting up a new Python context", e);
            throw new ExecutionException("Could not setup a new Python context", e);
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

    public void registerGlobalCode() {
        // The following code registers a global variable "loggerBridge" inside the Python context and
        // initializes it with our static LOGGER_BRIDGE member variable. This bridge is later used by
        // other Python code to initialize a "log" variable with the proper Python namespace and function
        // name.
        final var pyCode = """
                loggerBridge = None
                import polyglot
                @polyglot.export_value
                def register_ksml_logger_bridge(lb):
                  global loggerBridge
                  loggerBridge = lb
                """;
        final var register = registerFunction(pyCode, "register_ksml_logger_bridge");
        if (register == null) {
            throw new ExecutionException("Could not register global code for loggerBridge:\n" + pyCode);
        }
        // Load the global LOGGER_BRIDGE variable into the context
        register.execute(LOGGER_BRIDGE);
    }
}
