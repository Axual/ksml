package io.axual.ksml.python;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2025 Axual B.V.
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

import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.graalvm.polyglot.PolyglotException;
import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

@Slf4j
class PythonContextTest {

    /**
     * Parameterized test for host file access: when disabled, reading any file (even temp) is denied;
     * when enabled, reading the temp file succeeds. Uses @TempDir for automatic cleanup.
     * Writes a file and attempts to read it from Python code via registerFunction + execute().
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testHostFileAccess(boolean allowHostFileAccess, @TempDir Path tempDir) throws Exception {
        final var config = PythonContextConfig.builder()
                .allowHostFileAccess(allowHostFileAccess)
                .build();
        try (var pythonContext = new PythonContext(config)) {
            final var tmpFile = tempDir.resolve("ksml-test.txt");
            final var expected = "hello-ksml";
            Files.writeString(tmpFile, expected);
            final var path = tmpFile.toAbsolutePath().toString().replace("\\", "\\\\");

            final var pyCode = String.format("""
                    import polyglot
                    @polyglot.export_value
                    def test_read_file():
                        f = open('%s', 'r')
                        data = f.read()
                        f.close()
                        return data
                    """, path);
            final var fn = pythonContext.registerFunction(pyCode, "test_read_file");

            if (allowHostFileAccess) {
                // ✅ Expect successful read
                String result = fn.execute().asString();
                assertThat(result).isEqualTo(expected);
            } else {
                // ❌ Expect PolyglotException when file access is restricted
                assertThatThrownBy(fn::execute)
                        .isInstanceOf(PolyglotException.class)
                        .hasMessageContaining("Operation not permitted");
            }
        }
    }

    /**
     * Parameterized test for host socket access: when disabled, binding a socket is denied;
     * when enabled, binding and closing should succeed.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testHostSocketAccess(boolean allowHostSocketAccess) {
        final var config = PythonContextConfig.builder()
                .allowHostSocketAccess(allowHostSocketAccess)
                .build();
        try (var pythonContext = new PythonContext(config)) {
            final var pyCode = """
                    import polyglot
                    @polyglot.export_value
                    def test_socket():
                        import socket
                        s = socket.socket()
                        s.bind(('127.0.0.1', 0))
                        s.close()
                    """;
            final var fn = pythonContext.registerFunction(pyCode, "test_socket");

            if (allowHostSocketAccess) {
                // ✅ Expect socket bind to succeed
                fn.execute();
            } else {
                // ❌ Expect exception due to restricted access
                assertThatThrownBy(fn::execute)
                        .isInstanceOf(PolyglotException.class)
                        .hasMessageContaining("socket was excluded");
            }
        }
    }

    // Verifies whether the Python context can access system environment variables
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testEnvironmentVariableAccess(boolean inheritEnv) {
        final var config = PythonContextConfig.builder()
                .inheritEnvironmentVariables(inheritEnv)
                .build();
        try (var pythonContext = new PythonContext(config)) {
            final var pyCode = """
                    import polyglot
                    @polyglot.export_value
                    def test_env():
                        import os
                        return os.getenv('PATH')
                    """;
            final var fn = pythonContext.registerFunction(pyCode, "test_env");
            final var pathValue = fn.execute();

            assertThat(pathValue).isNotNull();
            if (inheritEnv) {
                // ✅ Expect PATH variable to be readable
                assertThat(pathValue.isNull()).isFalse();
                assertThat(pathValue.asString()).isNotEmpty();
            } else {
                // ❌ Expect no access, thus null
                assertThat(pathValue.isNull()).isTrue();
            }
        }
    }

    // Checks if thread creation is permitted based on the thread access setting
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testThreadCreation(boolean allowCreateThread) {
        final var config = PythonContextConfig.builder()
                .allowCreateThread(allowCreateThread)
                .build();
        try (var pythonContext = new PythonContext(config)) {
            final var pyCode = """
                    import polyglot
                    @polyglot.export_value
                    def test_thread():
                        import threading
                        t = threading.Thread(target=lambda: None)
                        t.start()
                        t.join()
                    """;
            final var fn = pythonContext.registerFunction(pyCode, "test_thread");

            if (allowCreateThread) {
                // ✅ Threading should work
                fn.execute();
                // Python's t.join() can return slightly before GraalVM has finished disposing
                // of the underlying host thread (see oracle/graal PolyglotLanguageContext#dispose,
                // which tracks "owned alive polyglot threads" separately). Closing the context
                // immediately after can then race and throw IllegalStateException. Wait for the
                // host thread to actually be gone before letting try-with-resources close it.
                awaitNoLingeringPolyglotThreads();
            } else {
                // ❌ Thread creation should be denied
                assertThatThrownBy(fn::execute)
                        .isInstanceOf(PolyglotException.class)
                        .hasMessageContaining("Creating threads is not allowed");
            }
        }
    }

    private static void awaitNoLingeringPolyglotThreads() {
        Awaitility.await("Wait for GraalVM's polyglot thread to fully finish")
                .atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(10))
                .until(() -> Thread.getAllStackTraces().keySet().stream()
                        .noneMatch(t -> t.getName().startsWith("Polyglot-python-")));
    }

    // Confirms whether native library access using ctypes is permitted. Only one GraalPy context per
    // process can load C extensions such as _ctypes, and a denied attempt also counts, so each case
    // runs in its own JVM.
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testNativeAccess(boolean allowNativeAccess) throws Exception {
        final var output = runInNewJvm(NativeAccessCheck.class, String.valueOf(allowNativeAccess));
        if (allowNativeAccess) {
            // ✅ Expect native function call to succeed
            assertThat(output).containsPattern("pid=[1-9][0-9]*");
        } else {
            // ❌ Expect native access to fail
            assertThat(output)
                    .contains("native access is not allowed")
                    .doesNotContain("pid=");
        }
    }

    static class NativeAccessCheck {
        public static void main(String[] args) {
            final var config = PythonContextConfig.builder()
                    .allowNativeAccess(Boolean.parseBoolean(args[0]))
                    .build();
            try (var pythonContext = new PythonContext(config)) {
                final var pyCode = """
                        import polyglot
                        @polyglot.export_value
                        def test_native():
                            import ctypes
                            lib = ctypes.CDLL(None)
                            return lib.getpid()
                        """;
                final var fn = pythonContext.registerFunction(pyCode, "test_native");
                try {
                    System.out.println("pid=" + fn.execute().asInt());
                } catch (PolyglotException e) {
                    System.out.println("error: " + e.getMessage());
                }
            }
        }
    }

    // The limit of GraalPy 25.1+ that the docs describe: only one context per process can load C
    // extension modules. Normal modules still work in the other contexts.
    @Test
    void onlyOneContextCanLoadNativeModules() throws Exception {
        final var output = runInNewJvm(NativeLimitCheck.class);
        assertThat(output)
                .containsPattern("first=pid=[1-9][0-9]*")
                .contains("second=not loaded")
                .contains("IsolateNativeModules")
                .contains("Only one Python context per KSML process can load C extension modules")
                .contains("normal modules=ok");
    }

    static class NativeLimitCheck {
        public static void main(String[] args) {
            final var config = PythonContextConfig.builder().allowNativeAccess(true).build();
            final var pyCode = """
                    import polyglot
                    import ctypes
                    @polyglot.export_value
                    def native_pid():
                        return ctypes.CDLL(None).getpid()
                    """;
            try (var first = new PythonContext(config); var second = new PythonContext(config)) {
                System.out.println("first=pid=" + first.registerFunction(pyCode, "native_pid").execute().asInt());
                final var fn = second.registerFunction(pyCode, "native_pid");
                System.out.println("second=" + (fn == null ? "not loaded" : "loaded"));
                second.context().eval("python", "import json, decimal, uuid, hashlib, zlib");
                System.out.println("normal modules=ok");
            }
        }
    }

    // Default config: every context must get ImportError for C extensions, not only the first.
    // GraalPy 25.5 takes its one C extension place before it checks native access.
    @Test
    void defaultConfigTwoContextsGetImportError() throws Exception {
        assertThat(runInNewJvm(DefaultConfigCheck.class))
                .contains("first ctypes=fallback")
                .contains("second ctypes=fallback")
                .contains("first getpass=ok")
                .contains("second getpass=ok");
    }

    static class DefaultConfigCheck {
        public static void main(String[] args) {
            final var pyCode = """
                    import polyglot
                    @polyglot.export_value
                    def try_imports():
                        try:
                            import ctypes
                            result = 'ctypes=loaded'
                        except ImportError:
                            result = 'ctypes=fallback'
                        import getpass
                        return result + ' getpass=ok'
                    """;
            for (var name : List.of("first", "second")) {
                try (var pythonContext = new PythonContext(PythonContextConfig.builder().build())) {
                    final var result = pythonContext.registerFunction(pyCode, "try_imports").execute().asString();
                    System.out.println(name + " " + result.replace(" ", " " + name + " "));
                } catch (Exception e) {
                    System.out.println(name + " error: " + e.getMessage());
                }
            }
        }
    }

    @Test
    void testNativeModuleHint() {
        final var limit = new RuntimeException("SystemError: Option python.IsolateNativeModules is set to 'false' and a second GraalPy context ...");
        final var later = new RuntimeException("SystemError: The C API initialization has previously failed.");
        try (var nativeOn = new PythonContext(PythonContextConfig.builder().allowNativeAccess(true).build());
             var nativeOff = new PythonContext(PythonContextConfig.builder().build())) {
            assertThat(nativeOn.nativeModuleHint(limit)).startsWith("Only one Python context per KSML process");
            assertThat(nativeOn.nativeModuleHint(later)).startsWith("Only one Python context per KSML process");
            assertThat(nativeOff.nativeModuleHint(limit)).isNull();
            assertThat(nativeOn.nativeModuleHint(new RuntimeException("NameError: name 'x' is not defined"))).isNull();
        }
    }

    // GraalPy 25.1+ asks for the current folder; the sandbox answers "/" and still denies relative paths.
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void currentFolderIsRootAndRelativePathsAreDenied(boolean withModulePath, @TempDir Path moduleDir) {
        final var config = PythonContextConfig.builder()
                .modulePath(withModulePath ? moduleDir.toString() : null)
                .build();
        try (var pythonContext = new PythonContext(config)) {
            final var pyCode = """
                    import polyglot
                    @polyglot.export_value
                    def check_current_folder():
                        import os
                        results = [os.getcwd()]
                        for mode in ('r', 'w'):
                            try:
                                open('x', mode)
                                results.append('allowed')
                            except PermissionError:
                                results.append('denied')
                        return ' '.join(results)
                    """;
            assertThat(pythonContext.registerFunction(pyCode, "check_current_folder").execute().asString())
                    .isEqualTo("/ denied denied");
        }
    }

    private static String runInNewJvm(Class<?> mainClass, String... args) throws Exception {
        final var command = new ArrayList<String>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        // Same JVM flags as this test JVM (native access, JaCoCo), without the debugger
        ManagementFactory.getRuntimeMXBean().getInputArguments().stream()
                .filter(arg -> !arg.startsWith("-agentlib:jdwp"))
                .forEach(command::add);
        command.addAll(List.of("-cp", System.getProperty("java.class.path"), mainClass.getName()));
        command.addAll(List.of(args));
        final var outputFile = Files.createTempFile("native-access-check", ".log");
        try {
            final var process = new ProcessBuilder(command)
                    .redirectErrorStream(true)
                    .redirectOutput(outputFile.toFile())
                    .start();
            final boolean finished = process.waitFor(5, TimeUnit.MINUTES);
            if (!finished) {
                process.destroyForcibly();
            }
            final var output = Files.readString(outputFile);
            assertThat(finished).as("JVM did not stop in time: %s", output).isTrue();
            assertThat(process.exitValue()).as(output).isZero();
            return output;
        } finally {
            Files.deleteIfExists(outputFile);
        }
    }

    // Subprocess creation is tricky to test positively outside a controlled environment,
    // due to OS security constraints.
    // This test will be easier to implement once a GraalVM containerized test environment is in place,
    // allowing better control over subprocess execution.
    @Test
    void verifyCreateProcessDenied() {
        final var config = PythonContextConfig.builder()
                .allowCreateProcess(false)
                .build();
        try (var pythonContext = new PythonContext(config)) {
            final var pyCode = """
                    import polyglot
                    @polyglot.export_value
                    def test_process_denied():
                        import subprocess
                        subprocess.check_output(['/bin/echo', 'hello'])
                    """;
            final var fn = pythonContext.registerFunction(pyCode, "test_process_denied");

            assertThatThrownBy(fn::execute)
                    .isInstanceOf(PolyglotException.class)
                    .hasMessageContaining("Operation not permitted");
        }
    }

    /**
     * Path-traversal sandbox check: with {@code allowHostFileAccess = false} and a configured
     * {@code modulePath}, Python must not be able to read directories outside the module path by
     * appending {@code ../..} to the supplied base path.
     * <p>
     * The Python function receives the absolute module path, appends {@code /..} ten times
     * (so the resolved target sits at or above the filesystem root), and calls {@link java.io.File}'s
     * listdir equivalent. A successful listing means the GraalVM read-only FileSystem failed to
     * contain the guest; the test fails in that case because no {@link PolyglotException} is thrown.
     */
    @Test
    @DisplayName("Python cannot escape modulePath via ../.. traversal when host file access is disabled")
    void pythonCannotEscapeModulePathViaDotDot(@TempDir Path moduleDir) {
        final var config = PythonContextConfig.builder()
                .allowHostFileAccess(false)
                .modulePath(moduleDir.toString())
                .build();
        try (var pythonContext = new PythonContext(config)) {
            final var pyCode = """
                    import polyglot
                    @polyglot.export_value
                    def try_escape(base):
                        import os
                        target = base
                        for _ in range(5):
                            target = target + '/..'
                        # If the sandbox holds, this raises; if it leaks, listdir returns
                        # the contents of the filesystem root.
                        return os.listdir(target)
                    """;
            final var fn = pythonContext.registerFunction(pyCode, "try_escape");

            // Test fails (no exception) if Python successfully listed a directory
            // outside the configured modulePath - i.e. the sandbox was escapable.
            var moduleDirStr = moduleDir.toString();
            assertThatThrownBy(() -> fn.execute(moduleDirStr))
                    .isInstanceOf(PolyglotException.class);
        }
    }

    /**
     * Companion to {@link #pythonCannotEscapeModulePathViaDotDot}: {@code sys.prefix} is in GraalVM's
     * resource cache, which {@code FileSystem.allowInternalResources} makes readable. A {@code ../..}
     * chain after it must not reach other host files.
     */
    @Test
    @DisplayName("Python cannot escape via sys.prefix prefix + ../.. when host file access is disabled")
    void pythonCannotEscapeViaSysPrefixDotDot(@TempDir Path moduleDir) {
        final var config = PythonContextConfig.builder()
                .allowHostFileAccess(false)
                .modulePath(moduleDir.toString())
                .build();
        try (var pythonContext = new PythonContext(config)) {
            // The Python guest itself constructs the target from sys.prefix. It returns a
            // diagnostic string on success so we can see exactly what leaked.
            final var pyCode = """
                    import polyglot
                    @polyglot.export_value
                    def try_escape_via_sys_prefix():
                        import sys, os
                        print("sys.prefix = " + sys.prefix)
                        target = sys.prefix + "/../../../../../"
                        resolved = os.path.realpath(target)
                        files = sorted(os.listdir(target))
                        return "path=" + target + " resolved=" + resolved \
                            + " file_count=" + str(len(files)) \
                            + " files_preview=" + str(files[:20])
                    """;
            final var fn = pythonContext.registerFunction(pyCode, "try_escape_via_sys_prefix");

            // If the call returns at all, the sandbox has been escaped - fail loudly with
            // the diagnostic from the guest. If a PolyglotException is thrown, the sandbox
            // contains the guest and the test passes.
            Value fnResult;
            try {
                fnResult = fn.execute();
            } catch (PolyglotException expected) {
                log.debug("Sandbox correctly blocked sys.prefix traversal: {}", expected.getMessage());
                return;
            }
            fail("Sandbox escape via sys.prefix succeeded - Python returned: " + fnResult.asString());
        }
    }

    @Test
    @DisplayName("Python module import should work with configured pythonModulePath")
    void testPythonModuleImport() throws Exception {
        // Create a temporary module directory
        final var tempDir = Files.createTempDirectory("python-modules");
        final var moduleFile = tempDir.resolve("test_module.py");
        Files.writeString(moduleFile, "def greet(name):\n    return f'Hello, {name}!'");

        // Create context with module path
        final var config = PythonContextConfig.builder()
                .modulePath(tempDir.toString())
                .build();

        try (var pythonContext = new PythonContext(config)) {
            pythonContext.debugPythonPath();

            // Test that we can import the module
            final var testCode = """
                    import polyglot
                    import test_module
                    
                    def test_import():
                       return test_module.greet('World')
                    
                    polyglot.export_value(test_import)
                    """;

            final var value = pythonContext.registerFunction(testCode, "test_import");
            assertThat(value).as("Function should be registered").isNotNull();
            assertThat(value.canExecute()).as("Registered value should be executable").isTrue();

            final var result = value.execute();
            assertThat(result).isNotNull();
            assertThat(result.asString()).isEqualTo("Hello, World!");
        }
    }
}
