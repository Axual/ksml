package io.axual.ksml.python;

import static org.assertj.core.api.Assertions.*;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.PolyglotException;
import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;

class PythonContextTest {

    // Reflection-based utility to access the underlying GraalVM Context for low-level evaluation
    private Context getPolyglotContext(PythonContext pythonContext) throws Exception {
        Field ctxField = PythonContext.class.getDeclaredField("context");
        ctxField.setAccessible(true);
        return (Context) ctxField.get(pythonContext);
    }

    /**
     * Parameterized test for host file access: when disabled, reading any file (even temp) is denied;
     * when enabled, reading temp file succeeds. Uses @TempDir for automatic cleanup.
     * Writes a file and attempts to read it from Python code using the Polyglot API.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testHostFileAccess(boolean allowHostFileAccess, @TempDir Path tempDir) throws Exception {
        PythonContextConfig config = PythonContextConfig.builder()
                .allowHostFileAccess(allowHostFileAccess)
                .build();
        PythonContext pythonContext = new PythonContext(config);
        Context ctx = getPolyglotContext(pythonContext);

        // Prepare a temporary file with known content
        Path tmpFile = tempDir.resolve("ksml-test.txt");
        String expected = "hello-ksml";
        Files.writeString(tmpFile, expected);

        // Python code to read the file
        String code = String.format(
                "f = open('%s','r')\n" +
                        "data = f.read()\n" +
                        "f.close()\n" +
                        "data",
                tmpFile.toAbsolutePath().toString().replace("\\", "\\\\")
        );

        if (allowHostFileAccess) {
            // ✅ Expect successful read
            String result = ctx.eval("python", code).asString();
            assertThat(result).isEqualTo(expected);
        } else {
            // ❌ Expect PolyglotException when file access is restricted
            assertThatThrownBy(() -> ctx.eval("python", code))
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
    void testHostSocketAccess(boolean allowHostSocketAccess) throws Exception {
        PythonContextConfig config = PythonContextConfig.builder()
                .allowHostSocketAccess(allowHostSocketAccess)
                .build();
        PythonContext pythonContext = new PythonContext(config);
        Context ctx = getPolyglotContext(pythonContext);

        String code =
                "import socket\n" +
                        "s = socket.socket()\n" +
                        "s.bind(('127.0.0.1', 0))\n" +
                        "s.close()";

        if (allowHostSocketAccess) {
            // ✅ Expect socket bind to succeed
            ctx.eval("python", code);
        } else {
            // ❌ Expect exception due to restricted access
            assertThatThrownBy(() -> ctx.eval("python", code))
                    .isInstanceOf(PolyglotException.class)
                    .hasMessageContaining("socket was excluded");
        }
    }

    // Verifies whether the Python context can access system environment variables
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testEnvironmentVariableAccess(boolean inheritEnv) throws Exception {
        PythonContextConfig config = PythonContextConfig.builder()
                .inheritEnvironmentVariables(inheritEnv)
                .build();
        PythonContext pythonContext = new PythonContext(config);
        Context ctx = getPolyglotContext(pythonContext);

        Value pathValue = ctx.eval("python", "import os; os.getenv('PATH')");
        assertThat(pathValue).isNotNull();

        if (inheritEnv) {
            // ✅ Expect PATH variable to be readable
            assertThat(pathValue.isNull()).isFalse();
            String path = pathValue.asString();
            assertThat(path).isNotEmpty();
        } else {
            // ❌ Expect conversion to string to fail
            assertThatThrownBy(pathValue::asString)
                    .isInstanceOf(ClassCastException.class)
                    .hasMessageContaining("Cannot convert");
        }
    }

    // Checks if thread creation is permitted based on the thread access setting
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testThreadCreation(boolean allowCreateThread) throws Exception {
        PythonContextConfig config = PythonContextConfig.builder()
                .allowCreateThread(allowCreateThread)
                .build();
        PythonContext pythonContext = new PythonContext(config);
        Context ctx = getPolyglotContext(pythonContext);

        String code =
                "import threading\n" +
                        "t = threading.Thread(target=lambda: None)\n" +
                        "t.start()\n" +
                        "t.join()";

        if (allowCreateThread) {
            // ✅ Threading should work
            ctx.eval("python", code);
        } else {
            // ❌ Thread creation should be denied
            assertThatThrownBy(() -> ctx.eval("python", code))
                    .isInstanceOf(PolyglotException.class)
                    .hasMessageContaining("Creating threads is not allowed");
        }
    }

    // Confirms whether native library access using ctypes is permitted
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testNativeAccess(boolean allowNativeAccess) throws Exception {
        PythonContextConfig config = PythonContextConfig.builder()
                .allowNativeAccess(allowNativeAccess)
                .build();
        PythonContext pythonContext = new PythonContext(config);
        Context ctx = getPolyglotContext(pythonContext);

        // ctypes.CDLL(None) is a portable way to load the standard C library and works on all major OSes
        String code = "import ctypes\nlib = ctypes.CDLL(None)\nlib.getpid()";

        if (allowNativeAccess) {
            // ✅ Expect native function call to succeed
            int pid = ctx.eval("python", code).asInt();
            assertThat(pid).isGreaterThan(0);
        } else {
            // ❌ Expect native access to fail
            assertThatThrownBy(() -> ctx.eval("python", code))
                    .isInstanceOf(PolyglotException.class)
                    .hasMessageContaining("cannot import");;
        }
    }

    // Subprocess creation is tricky to test positively outside a controlled environment,
    // due to OS security constraints.
    // This test will be easier to implement once a GraalVM containerized test environment is in place,
    // allowing better control over subprocess execution.
    @Test
    void verifyCreateProcessDenied() throws Exception {
        // Subprocess creation should be denied
        PythonContextConfig config = PythonContextConfig.builder()
                .allowCreateProcess(false)
                .build();
        PythonContext pythonContext = new PythonContext(config);
        Context ctx = getPolyglotContext(pythonContext);

        // Attempt to run a subprocess; expect denial
        assertThatThrownBy(() ->
                ctx.eval("python",
                        "import subprocess\n" +
                                "subprocess.check_output(['/bin/echo', 'hello'])")
        )
                .isInstanceOf(PolyglotException.class)
                .hasMessageContaining("Operation not permitted");;
    }
}
