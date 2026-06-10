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
import org.graalvm.polyglot.PolyglotException;
import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
class PythonContextTest {

    /**
     * Parameterized test for host file access: when disabled, reading any file (even temp) is denied;
     * when enabled, reading temp file succeeds. Uses @TempDir for automatic cleanup.
     * Writes a file and attempts to read it from Python code via registerFunction + execute().
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testHostFileAccess(boolean allowHostFileAccess, @TempDir Path tempDir) throws Exception {
        var config = PythonContextConfig.builder()
                .allowHostFileAccess(allowHostFileAccess)
                .build();
        var pythonContext = new PythonContext(config);

        Path tmpFile = tempDir.resolve("ksml-test.txt");
        String expected = "hello-ksml";
        Files.writeString(tmpFile, expected);
        String path = tmpFile.toAbsolutePath().toString().replace("\\", "\\\\");

        String pyCode = String.format("""
            import polyglot
            @polyglot.export_value
            def test_read_file():
                f = open('%s', 'r')
                data = f.read()
                f.close()
                return data
            """, path);
        Value fn = pythonContext.registerFunction(pyCode, "test_read_file");

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

    /**
     * Parameterized test for host socket access: when disabled, binding a socket is denied;
     * when enabled, binding and closing succeeds.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testHostSocketAccess(boolean allowHostSocketAccess) {
        var config = PythonContextConfig.builder()
                .allowHostSocketAccess(allowHostSocketAccess)
                .build();
        var pythonContext = new PythonContext(config);

        String pyCode = """
            import polyglot
            @polyglot.export_value
            def test_socket():
                import socket
                s = socket.socket()
                s.bind(('127.0.0.1', 0))
                s.close()
            """;
        Value fn = pythonContext.registerFunction(pyCode, "test_socket");

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

    // Verifies whether the Python context can access system environment variables
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testEnvironmentVariableAccess(boolean inheritEnv) {
        var config = PythonContextConfig.builder()
                .inheritEnvironmentVariables(inheritEnv)
                .build();
        var pythonContext = new PythonContext(config);

        String pyCode = """
            import polyglot
            @polyglot.export_value
            def test_env():
                import os
                return os.getenv('PATH')
            """;
        Value fn = pythonContext.registerFunction(pyCode, "test_env");
        Value pathValue = fn.execute();

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

    // Checks if thread creation is permitted based on the thread access setting
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testThreadCreation(boolean allowCreateThread) {
        var config = PythonContextConfig.builder()
                .allowCreateThread(allowCreateThread)
                .build();
        var pythonContext = new PythonContext(config);

        String pyCode = """
            import polyglot
            @polyglot.export_value
            def test_thread():
                import threading
                t = threading.Thread(target=lambda: None)
                t.start()
                t.join()
            """;
        Value fn = pythonContext.registerFunction(pyCode, "test_thread");

        if (allowCreateThread) {
            // ✅ Threading should work
            fn.execute();
        } else {
            // ❌ Thread creation should be denied
            assertThatThrownBy(fn::execute)
                    .isInstanceOf(PolyglotException.class)
                    .hasMessageContaining("Creating threads is not allowed");
        }
    }

    // Confirms whether native library access using ctypes is permitted
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testNativeAccess(boolean allowNativeAccess) {
        var config = PythonContextConfig.builder()
                .allowNativeAccess(allowNativeAccess)
                .build();
        var pythonContext = new PythonContext(config);

        String pyCode = """
            import polyglot
            @polyglot.export_value
            def test_native():
                import ctypes
                lib = ctypes.CDLL(None)
                return lib.getpid()
            """;
        Value fn = pythonContext.registerFunction(pyCode, "test_native");

        if (allowNativeAccess) {
            // ✅ Expect native function call to succeed
            int pid = fn.execute().asInt();
            assertThat(pid).isGreaterThan(0);
        } else {
            // ❌ Expect native access to fail
            assertThatThrownBy(fn::execute)
                    .isInstanceOf(PolyglotException.class)
                    .hasMessageContaining("cannot import");
        }
    }

    // Subprocess creation is tricky to test positively outside a controlled environment,
    // due to OS security constraints.
    // This test will be easier to implement once a GraalVM containerized test environment is in place,
    // allowing better control over subprocess execution.
    @Test
    void verifyCreateProcessDenied() {
        var config = PythonContextConfig.builder()
                .allowCreateProcess(false)
                .build();
        var pythonContext = new PythonContext(config);

        String pyCode = """
            import polyglot
            @polyglot.export_value
            def test_process_denied():
                import subprocess
                subprocess.check_output(['/bin/echo', 'hello'])
            """;
        Value fn = pythonContext.registerFunction(pyCode, "test_process_denied");

        assertThatThrownBy(fn::execute)
                .isInstanceOf(PolyglotException.class)
                .hasMessageContaining("Operation not permitted");
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
        var config = PythonContextConfig.builder()
                .allowHostFileAccess(false)
                .modulePath(moduleDir.toString())
                .build();
        var pythonContext = new PythonContext(config);

        String pyCode = """
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
        Value fn = pythonContext.registerFunction(pyCode, "try_escape");

        // Test fails (no exception) if Python successfully listed a directory
        // outside the configured modulePath - i.e. the sandbox was escapable.
        var moduleDirStr = moduleDir.toString();
        assertThatThrownBy(() -> fn.execute(moduleDirStr))
                .isInstanceOf(PolyglotException.class);
    }

    /**
     * Companion to {@link #pythonCannotEscapeModulePathViaDotDot}: the read-only FileSystem
     * built in {@link PythonContext} permits any path whose elements start with either
     * {@code modulePath} or {@code sys.prefix}. The {@code sys.prefix} branch is the
     * interesting one because GraalPy's python-home lives at a canonical, predictable
     * location. A literal target like
     * <pre>{@code <sys.prefix>/../../../../../}</pre>
     * passes the element-wise {@code Path.startsWith} check (its leading elements <em>are</em>
     * the elements of {@code sys.prefix}), so the only thing standing between the guest and
     * the host filesystem is whether GraalVM normalizes the path before the selector predicate
     * fires.
     * <p>
     * The {@code ..} chain is appended directly to {@code sys.prefix} - no intermediate
     * directories - so the test has no dependency on the installed GraalPy version layout.
     * The test fails (with the leaked directory listing) if the guest successfully read
     * outside the sandbox.
     */
    @Test
    @DisplayName("Python cannot escape via sys.prefix prefix + ../.. when host file access is disabled")
    void pythonCannotEscapeViaSysPrefixDotDot(@TempDir Path moduleDir) {
        var config = PythonContextConfig.builder()
                .allowHostFileAccess(false)
                .modulePath(moduleDir.toString())
                .build();
        var pythonContext = new PythonContext(config);

        // The Python guest itself constructs the target from sys.prefix. It returns a
        // diagnostic string on success so we can see exactly what leaked.
        String pyCode = """
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
        Value fn = pythonContext.registerFunction(pyCode, "try_escape_via_sys_prefix");

        // If the call returns at all, the sandbox has been escaped - fail loudly with
        // the diagnostic from the guest. If a PolyglotException is thrown, the sandbox
        // contained the guest and the test passes.
        Value fnResult;
        try {
            fnResult = fn.execute();
        } catch (PolyglotException expected) {
            log.debug("Sandbox correctly blocked sys.prefix traversal: {}", expected.getMessage());
            return;
        }
        Assertions.fail("Sandbox escape via sys.prefix succeeded - Python returned: " + fnResult.asString());
    }

    @Test
    @DisplayName("Python module import should work with configured pythonModulePath")
    void testPythonModuleImport() throws Exception {
        // Create a temporary module directory
        Path tempDir = Files.createTempDirectory("python-modules");
        Path moduleFile = tempDir.resolve("test_module.py");
        Files.writeString(moduleFile, "def greet(name):\n    return f'Hello, {name}!'");

        // Create context with module path
        var config = PythonContextConfig.builder()
                .modulePath(tempDir.toString())
                .build();

        var pythonContext = new PythonContext(config);

        pythonContext.debugPythonPath();

        // Test that we can import the module
        String testCode = """
          import polyglot
          import test_module
          
          def test_import():
             return test_module.greet('World')
             
          polyglot.export_value(test_import)
          """;

        var value = pythonContext.registerFunction(testCode, "test_import");
        assertNotNull(value, "Function should be registered");
        assertTrue(value.canExecute(), "Registered value should be executable");

        var result = value.execute();
        assertThat(result).isNotNull();
        assertThat(result.asString()).isEqualTo("Hello, World!");
    }

}
