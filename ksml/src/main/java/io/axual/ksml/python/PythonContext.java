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

import org.apache.commons.lang3.StringUtils;
import org.graalvm.polyglot.io.FileSystem;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.EnvironmentAccess;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.PolyglotAccess;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.io.IOAccess;

import java.nio.file.Path;
import java.util.List;

@Slf4j
public class PythonContext {
    private static final LoggerBridge LOGGER_BRIDGE = new LoggerBridge();
    private static final MetricsBridge METRICS_BRIDGE = new MetricsBridge(Metrics.registry());
    private static final String PYTHON = "python";
    // With HostAccess.EXPLICIT, only classes with @HostAccess.Export annotations are accessible
    // Java collections (ArrayList, HashMap, TreeMap) are no longer needed since PythonTypeConverter
    // converts them to Python-native types before passing to Python code
    private static final List<String> ALLOWED_JAVA_CLASSES = List.of(
            "io.axual.ksml.python.LoggerBridge$PythonLogger",
            "io.axual.ksml.python.MetricsBridge",
            "io.axual.ksml.python.CounterBridge",
            "io.axual.ksml.python.MeterBridge",
            "io.axual.ksml.python.TimerBridge",
            "io.axual.ksml.store.KeyValueStoreProxy",
            "io.axual.ksml.store.SessionStoreProxy",
            "io.axual.ksml.store.WindowStoreProxy",
            "io.axual.ksml.store.TimestampedWindowStoreProxy",
            "io.axual.ksml.store.TimestampedKeyValueStoreProxy",
            "io.axual.ksml.store.VersionedKeyValueStoreProxy",
            "io.axual.ksml.store.VersionedRecordProxy",
            "io.axual.ksml.store.ValueAndTimestampProxy");
    private final Context context;
    @Getter
    private final DataObjectConverter converter;

    public PythonContext(PythonContextConfig config) {
        this.converter = new DataObjectConverter();

        log.debug("Setting up new Python context: {}", config);
        try {
            var contextBuilder = Context.newBuilder(PYTHON)
                    .allowIO(IOAccess.newBuilder()
                            .allowHostFileAccess(config.allowHostFileAccess())
                            .allowHostSocketAccess(config.allowHostSocketAccess())
                            .build())
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
                    .allowHostAccess(HostAccess.EXPLICIT)
                    .allowHostClassLookup(ALLOWED_JAVA_CLASSES::contains);

            // set up configured I/O access
            IOAccess ioAccess = createIOAccess(config.allowHostFileAccess(), config.allowHostSocketAccess(), config.modulePath());

            context = contextBuilder
                    .allowIO(ioAccess)
                    .build();

            if (!StringUtils.isEmpty( config.modulePath() )) {
//                if (!Files.isDirectory( Path.of(config.pythonModulePath()))) {
//                    log.error("Configured Python module path {} does not exist or is not a directory", config.pythonModulePath());
//                    throw new ExecutionException("Configured Python module path does not exist or is not a directory");
//                }
                addModulePathToSysPath(Path.of(config.modulePath()));
            }

            registerGlobalCode();
        } catch (Exception e) {
            log.error("Error setting up a new Python context", e);
            throw new ExecutionException("Could not setup a new Python context", e);
        }
    }

    /**
     * Register a function in the Python context.
     * @param pyCode the function source code.
     * @param callerName the name of the function to be registered.
     * @return a GraalVM {@link Value} object that can be used to call the registered function.
     */
    public Value registerFunction(String pyCode, String callerName) {
        Source script = Source.create(PYTHON, pyCode);
        try {
            context.eval(script);
        } catch (Exception e) {
            log.error("Error loading Python code", e);
        }
        return context.getPolyglotBindings().getMember(callerName);
    }

    /**
     * Register global code that is used to initialize the loggerBridge and metricsBridge variables.
     */
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

    /**
     * Create an GraalVM IOAccess object based on the settings in the configuration.
     * @param allowHostFileAccess indicate if host file access should be allowed.
     * @param allowHostSocketAccess indicate if host socket access should be allowed.
     * @param modulePath the path where customer Python modules are located.
     * @return an IOAccess object based on the settings in the configuration.
     */
    private IOAccess createIOAccess(boolean allowHostFileAccess, boolean allowHostSocketAccess, String modulePath) {
        log.debug("createIOAccess({}, {}, {})", allowHostFileAccess, allowHostSocketAccess, modulePath);

        if (allowHostFileAccess || StringUtils.isEmpty( modulePath)) {
            // if host file access is allowed, then we don't need to set up a restricted file system for module access
            return IOAccess.newBuilder()
                    .allowHostFileAccess(allowHostFileAccess)
                    .allowHostSocketAccess(allowHostSocketAccess)
                    .build();
        } else {
            // host file access is false, but modulePath is not empty: set up restricted file system for module access
            FileSystem modulesReadOnly = createReadOnlyFileSystem(modulePath);
            return IOAccess.newBuilder()
                    .fileSystem(modulesReadOnly)
                    .allowHostSocketAccess(allowHostSocketAccess)
                    .build();
        }
    }

    /**
     * Create an FileSystem object that allows read-only access to Python's home, and the given path and everything below it.
     * @param modulePath the path where customer Python modules are located.
     * @return an FileSystem for Python module access that allows read-only access to the given path and everything below it.
     */
    private FileSystem createReadOnlyFileSystem(String modulePath) {
        log.debug("createReadOnlyFileSystem({})", modulePath);
        FileSystem modulesFileSystem = FileSystem.newDefaultFileSystem();
        FileSystem denyAllAccess = FileSystem.newDenyIOFileSystem();
        String sysPrefix = getPythonSysPrefix();
        FileSystem restricted = FileSystem.newCompositeFileSystem(
                // the default/fallback file system is: deny access
                denyAllAccess,
                // add a selector that returns the modulesFileSystem only if the path matches modulePath or sys.prefix
                FileSystem.Selector.of(modulesFileSystem, path -> path.startsWith(modulePath) || path.startsWith(sysPrefix))
        );

        return FileSystem.newReadOnlyFileSystem(restricted);
    }

    /**
     * Get sys.prefix from GraalVM Python.
     * @return the value of sys.prefix.
     */
    private String getPythonSysPrefix() {
        log.debug("getPythonSysPrefix()");
        var tempContext = Context.newBuilder(PYTHON).build();
        var result = tempContext.eval(PYTHON, "import sys; sys.prefix").asString();
        log.debug("getPythonSysPrefix() ---> {}", result);
        tempContext.close();
        return result;
    }

    /**
     * Add a directory to Python's sys.path for module imports
     */
    private void addModulePathToSysPath(Path modulePath) {
        log.debug("addModulePathToSysPath({})", modulePath.toAbsolutePath().toString().replace("\\", "\\\\").replace("'", "\\'"));
        try {
            String absolutePath = modulePath.toAbsolutePath().toString();
            String pythonCode = String.format(
                """
                    import sys
                    if '%s' not in sys.path:
                        sys.path.insert(0, '%s')
                    """,
                    absolutePath.replace("\\", "\\\\").replace("'", "\\'"),
                    absolutePath.replace("\\", "\\\\").replace("'", "\\'")
            );

            context.eval(PYTHON, pythonCode);
            log.info("Added Python module path to sys.path: {}", absolutePath);
        } catch (Exception e) {
            log.error("Failed to add module path to sys.path: {}", modulePath, e);
            throw new ExecutionException("Could not configure Python module path", e);
        }
    }

    /**
     * Show the configured Python sys.path for debugging.
     */
    public void debugPythonPath() {
        try {
            var result = context.eval("python", "import sys; sys.path");
            log.debug("Python sys.path: {}", result);
        } catch (Exception e) {
            log.warn("Could not read Python sys.path", e);
        }
    }

}
