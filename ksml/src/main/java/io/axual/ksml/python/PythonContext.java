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
import io.axual.ksml.exception.ExecutionException;
import io.axual.ksml.metric.Metrics;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.graalvm.polyglot.*;
import org.graalvm.polyglot.io.IOAccess;

import java.util.List;

@Slf4j
public class PythonContext {
    private static final LoggerBridge LOGGER_BRIDGE = new LoggerBridge();
    private static final MetricsBridge METRICS_BRIDGE = new MetricsBridge(Metrics.registry());
    private static final String PYTHON = "python";
    private static final List<String> ALLOWED_JAVA_CLASSES = List.of(
            "java.util.ArrayList",
            "java.util.HashMap",
            "java.util.TreeMap");
    private final Context context;
    @Getter
    private final DataObjectConverter converter;

    public PythonContext(PythonContextConfig config) {
        this.converter = new DataObjectConverter();

        log.debug("Setting up new Python context: {}", config);
        try {
            context = Context.newBuilder(PYTHON)
                    .allowIO(IOAccess.newBuilder()
                            .allowHostFileAccess(config.allowHostFileAccess())
                            .allowHostSocketAccess(config.allowHostSocketAccess())
                            .build())
//                    .allowIO(IOAccess.ALL)
                    .allowNativeAccess(config.allowNativeAccess())
                    .allowCreateProcess(config.allowCreateProcess())
                    .allowCreateThread(config.allowCreateThread())
                    .allowEnvironmentAccess(
                            config.inheritEnvironmentVariables()
                                    ? EnvironmentAccess.INHERIT
                                    : EnvironmentAccess.NONE)
                    .allowPolyglotAccess(
                            PolyglotAccess.newBuilder()
                                    .allowBindingsAccess(PYTHON)
                                    .build())
//                    .allowPolyglotAccess(PolyglotAccess.ALL)
                    .allowHostAccess(
                            HostAccess.newBuilder()
                                    .allowPublicAccess(true)
                                    .allowAllImplementations(false)
                                    .allowAllClassImplementations(false)
                                    .allowArrayAccess(false)
                                    .allowListAccess(true)
                                    .allowBufferAccess(false)
                                    .allowIterableAccess(true)
                                    .allowIteratorAccess(true)
                                    .allowMapAccess(true)
                                    .allowAccessInheritance(false)
                                    .build())
//                    .allowHostAccess(HostAccess.ALL)
                    .allowHostClassLookup(ALLOWED_JAVA_CLASSES::contains)
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
        // The following code registers a global variables "loggerBridge" and "metricsBridge" inside the Python
        // context and initializes it with our static LOGGER_BRIDGE member variable. This bridge is later used
        // by other Python code to initialize a "log" variable with the proper Python namespace and function name.
        final var pyCode = """
                loggerBridge = None
                metrics = None
                import polyglot

                @polyglot.export_value
                def register_ksml_bridges(lb, mb):
                  global loggerBridge
                  loggerBridge = lb
                  global metrics
                  metrics = mb
                """;
        final var register = registerFunction(pyCode, "register_ksml_bridges");
        if (register == null) {
            throw new ExecutionException("Could not register global code for loggerBridge:\n" + pyCode);
        }
        // Pass the global LOGGER_BRIDGE and METRICS_BRIDGE variables into global variables of the Python context
        register.execute(LOGGER_BRIDGE, METRICS_BRIDGE);
    }
}
