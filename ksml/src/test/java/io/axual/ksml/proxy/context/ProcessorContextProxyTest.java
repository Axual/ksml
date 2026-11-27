package io.axual.ksml.proxy.context;

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

import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.object.DataLong;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.python.PythonContext;
import io.axual.ksml.python.PythonContextConfig;
import io.axual.ksml.type.UserType;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ProcessorContextProxyTest {
    private static final long PUNCTUATION_TIME = 60_000L;
    private static final UserType STRING = new UserType(DataString.DATATYPE);
    private static final UserType LONG = new UserType(DataLong.DATATYPE);
    private static final ProcessorContextProxy.Target NEXT = new ProcessorContextProxy.Target("main", STRING, LONG);
    private static final ProcessorContextProxy.Target ENRICH = new ProcessorContextProxy.Target("punctuate-to-enrich", STRING, STRING);
    private static final ProcessorContextProxy.Target COUNT = new ProcessorContextProxy.Target("punctuate-to-count", STRING, LONG);

    private static PythonContext pythonContext;

    @Mock
    private ProcessorContext<Object, Object> context;

    @BeforeAll
    static void setUpPython() {
        pythonContext = new PythonContext(PythonContextConfig.builder().build());
        pythonContext.context().eval("python", "from ksml import " + ProcessorContextProxy.INJECT_CONTEXT.proxyClassName());
    }

    @AfterAll
    static void tearDownPython() {
        pythonContext.close();
    }

    private ProcessorContextProxy proxy() {
        return new ProcessorContextProxy(context, PUNCTUATION_TIME, NEXT, Map.of("enrich", ENRICH, "count", COUNT));
    }

    private Record<Object, Object> forwarded(String childName) {
        @SuppressWarnings("unchecked") final ArgumentCaptor<Record<Object, Object>> captor = ArgumentCaptor.forClass(Record.class);
        verify(context).forward(captor.capture(), org.mockito.ArgumentMatchers.eq(childName));
        return captor.getValue();
    }

    /**
     * Runs Python code with {@code context} bound to the proxy as functions see it, wrapped in the Python context class,
     * and {@code proxy} bound to the proxy itself; returns its result as a string, or "ErrorType: message".
     */
    private static String python(ProcessorContextProxy proxy, String call) {
        pythonContext.context().getBindings("python").putMember("proxy", proxy);
        return pythonContext.context().eval("python", """
                def run():
                    context = %s(proxy)
                    try:
                        return str(%s)
                    except Exception as e:
                        return type(e).__name__ + ": " + str(e)
                run()
                """.formatted(ProcessorContextProxy.INJECT_CONTEXT.proxyClassName(), call)).asString();
    }

    @Test
    @DisplayName("forward sends a record with the punctuation timestamp and no headers to the next operation")
    void forwardDefaults() {
        proxy().forward("a", 10, null, null, null);
        final var rec = forwarded("main");
        assertThat(rec.key()).isEqualTo(new DataString("a"));
        assertThat(rec.value()).isEqualTo(new DataLong(10L));
        assertThat(rec.timestamp()).isEqualTo(PUNCTUATION_TIME);
        assertThat(rec.headers().toArray()).isEmpty();
    }

    @Test
    @DisplayName("forward without a child name for the next operation forwards to all children")
    void forwardToAllChildren() {
        new ProcessorContextProxy(context, PUNCTUATION_TIME, new ProcessorContextProxy.Target(null, STRING, LONG), Map.of())
                .forward("a", 10, null, null, null);
        verify(context).forward(any(Record.class));
        verify(context, never()).forward(any(Record.class), anyString());
    }

    @Test
    @DisplayName("forward with a None value forwards a tombstone")
    void forwardTombstone() {
        proxy().forward("a", null, null, null, null);
        assertThat(forwarded("main").value()).isNull();
    }

    @Test
    @DisplayName("forward from Python passes timestamp, headers and to as keyword arguments")
    void forwardKeywordArguments() {
        final var result = python(proxy(), "context.forward('a', 'x', timestamp=1700000000000, headers=[{'key': 'source', 'value': 'flush'}], to='enrich')");
        assertThat(result).isEqualTo("None");
        final var rec = forwarded("punctuate-to-enrich");
        assertThat(rec.value()).isEqualTo(new DataString("x"));
        assertThat(rec.timestamp()).isEqualTo(1700000000000L);
        assertThat(rec.headers()).isEqualTo(new RecordHeaders().add("source", "flush".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    @DisplayName("forward from Python with only positional arguments uses the defaults")
    void forwardPositionalFromPython() {
        assertThat(python(proxy(), "context.forward('a', 10)")).isEqualTo("None");
        assertThat(forwarded("main").timestamp()).isEqualTo(PUNCTUATION_TIME);
    }

    @Test
    @DisplayName("forward raises TypeError naming both types when the value does not fit the next operation")
    void forwardWrongValueType() {
        assertThat(python(proxy(), "context.forward('a', 'ten')"))
                .startsWith("TypeError: forward value for the next operation must be")
                .contains("long").contains("str");
        verify(context, never()).forward(any(Record.class), anyString());
    }

    @Test
    @DisplayName("forward checks the types of the named target, not of the next operation")
    void forwardTypesOfTarget() {
        assertThat(python(proxy(), "context.forward('a', 'ten', to='count')"))
                .startsWith("TypeError: forward value for operation 'count' must be");
        verify(context, never()).forward(any(Record.class), anyString());
        assertThat(python(proxy(), "context.forward('a', 'x', to='enrich')")).isEqualTo("None");
        assertThat(forwarded("punctuate-to-enrich").value()).isEqualTo(new DataString("x"));
    }

    @Test
    @DisplayName("forward raises TypeError when the key does not fit")
    void forwardWrongKeyType() {
        final var longKeys = new ProcessorContextProxy.Target("main", LONG, LONG);
        assertThat(python(new ProcessorContextProxy(context, PUNCTUATION_TIME, longKeys, Map.of()), "context.forward('k', 10)"))
                .startsWith("TypeError: forward key for the next operation must be");
    }

    @Test
    @DisplayName("forward to an unknown name raises ValueError listing the valid names")
    void forwardUnknownTarget() {
        assertThat(python(proxy(), "context.forward('a', 'x', to='compute')"))
                .isEqualTo("ValueError: forward can not send to 'compute', valid names are: count, enrich");
        assertThat(python(new ProcessorContextProxy(context, PUNCTUATION_TIME, NEXT, Map.of()), "context.forward('a', 10, to='x')"))
                .endsWith("valid names are: none");
        assertThat(python(proxy(), "context.forward('a', 10, to=1)")).startsWith("TypeError: forward to must be an operation name");
        verifyNoInteractions(context);
    }

    @ParameterizedTest
    @ValueSource(strings = {"'source'", "['source']", "[{'key': 'source'}]", "[{'key': 'k', 'value': 1.5}]"})
    @DisplayName("forward with malformed headers raises TypeError and forwards nothing")
    void forwardMalformedHeaders(String headers) {
        assertThat(python(proxy(), "context.forward('a', 10, headers=" + headers + ")"))
                .startsWith("TypeError: forward headers must be a list of dicts with a key and a value");
        verifyNoInteractions(context);
    }

    @Test
    @DisplayName("forward with a bad timestamp raises an error and forwards nothing")
    void forwardBadTimestamp() {
        assertThat(python(proxy(), "context.forward('a', 10, timestamp='now')")).startsWith("TypeError: forward timestamp must be an int");
        assertThat(python(proxy(), "context.forward('a', 10, timestamp=-1)")).startsWith("ValueError: forward timestamp must not be negative");
        assertThat(python(proxy(), "context.forward('a', 10, timestamp=10**30)")).startsWith("ValueError: forward timestamp is out of range");
        verifyNoInteractions(context);
    }

    @Test
    @DisplayName("the other methods delegate to the Kafka Streams context")
    void delegatingMethods() {
        when(context.currentSystemTimeMs()).thenReturn(1L);
        when(context.currentStreamTimeMs()).thenReturn(2L);
        when(context.applicationId()).thenReturn("app");
        when(context.taskId()).thenReturn(new TaskId(0, 2));
        final var proxy = proxy();
        assertThat(python(proxy, "[context.currentSystemTimeMs(), context.currentStreamTimeMs(), context.applicationId(), context.taskId()]"))
                .isEqualTo("[1, 2, 'app', '0_2']");
        assertThat(python(proxy, "context.commit()")).isEqualTo("None");
        verify(context).commit();
    }

    @ParameterizedTest
    @ValueSource(strings = {"schedule", "getStateStore", "appConfigs", "appConfigsWithPrefix", "keySerde", "stateDir", "metrics", "recordMetadata", "getClass"})
    @DisplayName("Kafka Streams context methods that are not exported raise AttributeError, on the context and on the proxy")
    void hiddenMethods(String method) {
        assertThat(python(proxy(), "context." + method)).startsWith("AttributeError");
        assertThat(python(proxy(), "context._proxy." + method)).startsWith("AttributeError");
    }

    @Test
    @DisplayName("Python sees exactly the exported methods and can not change them")
    void visibleMembers() {
        final var methods = "['applicationId', 'commit', 'currentStreamTimeMs', 'currentSystemTimeMs', 'forward', 'taskId']";
        assertThat(python(proxy(), "sorted(m for m in dir(context) if not m.startswith('_'))")).isEqualTo(methods);
        assertThat(python(proxy(), "sorted(m for m in dir(proxy) if not m.startswith('_'))")).isEqualTo(methods);
        assertThat(python(proxy(), "setattr(context, 'forward', None)")).isEqualTo("AttributeError: The context is read-only");
        assertThat(python(proxy(), "proxy.invalidate()")).startsWith("AttributeError");
    }

    @Test
    @DisplayName("every method raises an error after invalidate, and forwards nothing")
    void invalidated() {
        final var proxy = proxy();
        proxy.invalidate();
        for (var call : List.of("context.forward('a', 10)", "context.commit()", "context.currentSystemTimeMs()",
                "context.currentStreamTimeMs()", "context.applicationId()", "context.taskId()")) {
            assertThat(python(proxy, call)).as(call).startsWith("RuntimeError: The context is no longer valid");
        }
        verifyNoInteractions(context);
    }

    @Test
    @DisplayName("called from Java, errors are DataExceptions")
    void errorsOutsidePython() {
        final var proxy = proxy();
        assertThatThrownBy(() -> proxy.forward("a", "ten", null, null, null)).isInstanceOf(DataException.class);
        proxy.invalidate();
        assertThatThrownBy(proxy::taskId).isInstanceOf(DataException.class).hasMessageContaining("no longer valid");
    }

    @Test
    @DisplayName("forward converts a Python float with a fraction to a double value")
    void forwardDouble() {
        final var doubles = new ProcessorContextProxy.Target("main", STRING, new UserType(io.axual.ksml.data.object.DataDouble.DATATYPE));
        assertThat(python(new ProcessorContextProxy(context, PUNCTUATION_TIME, doubles, Map.of()), "context.forward('a', 1.5)")).isEqualTo("None");
        assertThat(forwarded("main").value()).isEqualTo(new io.axual.ksml.data.object.DataDouble(1.5));
    }
}
