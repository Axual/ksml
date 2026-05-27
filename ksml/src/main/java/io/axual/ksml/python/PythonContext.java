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
import io.axual.ksml.proxy.log.LoggerBridge;
import io.axual.ksml.proxy.metric.MetricsBridge;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.EnvironmentAccess;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.PolyglotAccess;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.io.FileSystem;
import org.graalvm.polyglot.io.IOAccess;

import java.nio.file.Path;
import java.util.List;

@Slf4j
public class PythonContext implements AutoCloseable {

    private static final LoggerBridge LOGGER_BRIDGE = new LoggerBridge();
    private static final MetricsBridge METRICS_BRIDGE = new MetricsBridge(Metrics.registry());
    private static final String PYTHON = "python";

    // With HostAccess.EXPLICIT, only classes with @HostAccess.Export annotations are accessible
    // Java collections (ArrayList, HashMap, TreeMap) are no longer needed since PythonTypeConverter
    // converts them to Python-native types before passing to Python code
    private static final List<String> ALLOWED_JAVA_CLASSES = List.of(
            "io.axual.ksml.proxy.log.LoggerBridge$PythonLogger",
            "io.axual.ksml.proxy.metric.MetricsBridge",
            "io.axual.ksml.proxy.metric.CounterBridge",
            "io.axual.ksml.proxy.metric.MeterBridge",
            "io.axual.ksml.proxy.metric.TimerBridge",
            "io.axual.ksml.proxy.store.KeyValueStoreProxy",
            "io.axual.ksml.proxy.store.SessionStoreProxy",
            "io.axual.ksml.proxy.store.WindowStoreProxy",
            "io.axual.ksml.proxy.store.TimestampedWindowStoreProxy",
            "io.axual.ksml.proxy.store.TimestampedKeyValueStoreProxy",
            "io.axual.ksml.proxy.store.VersionedKeyValueStoreProxy",
            "io.axual.ksml.proxy.store.KeyValueIteratorProxy");
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

            // Canonicalize the configured modulePath once, up front. toRealPath() resolves
            // symlinks (notably macOS /var -> /private/var) so the same canonical string
            // flows into both the sys.path injection below and the FileSystem selector
            // prefix in createReadOnlyFileSystem. Without this, the two would disagree and
            // legitimate module imports would be denied on platforms where the configured
            // path traverses a symlink.
            final String canonicalModulePath = StringUtils.isEmpty(config.modulePath())
                    ? config.modulePath()
                    : Path.of(config.modulePath()).toRealPath().toString();

            // set up configured I/O access
            IOAccess ioAccess = createIOAccess(config.allowHostFileAccess(), config.allowHostSocketAccess(), canonicalModulePath);

            context = contextBuilder
                    .allowIO(ioAccess)
                    .build();

            if (!StringUtils.isEmpty(canonicalModulePath)) {
                addModulePathToSysPath(Path.of(canonicalModulePath));
            }

            registerGlobalCode();
        } catch (Exception e) {
            log.error("Error setting up a new Python context", e);
            throw new ExecutionException("Could not setup a new Python context", e);
        }
    }

    /**
     * Register a function in the Python context.
     *
     * @param pyCode     the function source code.
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
     *
     * @param allowHostFileAccess   indicate if host file access should be allowed.
     * @param allowHostSocketAccess indicate if host socket access should be allowed.
     * @param modulePath            the path where customer Python modules are located.
     * @return an IOAccess object based on the settings in the configuration.
     */
    private IOAccess createIOAccess(boolean allowHostFileAccess, boolean allowHostSocketAccess, String modulePath) {
        log.debug("createIOAccess({}, {}, {})", allowHostFileAccess, allowHostSocketAccess, modulePath);

        if (allowHostFileAccess || StringUtils.isEmpty(modulePath)) {
            // if host file access is allowed or no module path configured, then we don't need to set up an extra restricted file system for module access
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
     * <p>
     * The selector predicate normalizes both the candidate path and the two allowed prefixes
     * (the configured {@code modulePath} and Python's {@code sys.prefix}) before doing the
     * element-wise {@link Path#startsWith(Path)} check, to prevent any literal {@code ../..}
     * chain in the candidate path from escaping.
     * <p>
     * Normalization is purely lexical ({@link Path#normalize()}), not symlink-resolving
     * ({@code toRealPath}). That is safe under the threat model "untrusted Python in the
     * KSML definition" because the wrapping {@link FileSystem#newReadOnlyFileSystem} denies
     * the guest any write capability - in particular it cannot create symlinks inside the
     * allowed region to point outwards. Any symlinks that do exist in {@code modulePath} or
     * {@code sys.prefix} were placed there by the operator/image author and are considered
     * intentional. If write access to the allowed region is ever granted to the guest, this
     * predicate needs to be upgraded to symlink-resolving canonicalization.
     * <p>
     * The {@code modulePath} string passed in is expected to already be canonical
     * (resolved via {@link Path#toRealPath()} in the constructor).
     *
     * @param modulePath the canonical path where customer Python modules are located.
     * @return an FileSystem for Python module access that allows read-only access to the given path and everything below it.
     */
    private FileSystem createReadOnlyFileSystem(String modulePath) {
        log.debug("createReadOnlyFileSystem({})", modulePath);
        FileSystem modulesFileSystem = FileSystem.newDefaultFileSystem();
        FileSystem denyAllAccess = FileSystem.newDenyIOFileSystem();
        // Pre-normalize the allowed prefixes once - the per-call selector then only has to
        // normalize the candidate path and run two startsWith checks.
        final Path modulePathNormalized = Path.of(modulePath).toAbsolutePath().normalize();
        final Path sysPrefixNormalized = Path.of(getPythonSysPrefix()).toAbsolutePath().normalize();
        FileSystem restricted = FileSystem.newCompositeFileSystem(
                // the default/fallback file system is: deny access
                denyAllAccess,
                // add a selector that returns the modulesFileSystem only if the normalized path
                // is within modulePath or sys.prefix
                FileSystem.Selector.of(modulesFileSystem, path -> {
                    if (path == null || !path.isAbsolute()) {
                        return false;
                    }
                    Path normalized = path.toAbsolutePath().normalize();
                    if (!normalized.startsWith(modulePathNormalized) && !normalized.startsWith(sysPrefixNormalized)) {
                        log.warn("Refusing path {} which is outside of modulePath {} or sys.prefix {}", normalized, modulePathNormalized, sysPrefixNormalized);
                        return false;
                    }
                    return true;
                })
        );

        return FileSystem.newReadOnlyFileSystem(restricted);
    }

    /**
     * Get sys.prefix from GraalVM Python.
     *
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
            var result = context.eval(PYTHON, "import sys; sys.path");
            log.debug("Python sys.path: {}", result);
        } catch (Exception e) {
            log.warn("Could not read Python sys.path", e);
        }
    }

    @Override
    public void close() {
        context.close();
    }
}
