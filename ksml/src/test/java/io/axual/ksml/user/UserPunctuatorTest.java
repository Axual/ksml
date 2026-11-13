package io.axual.ksml.user;

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

import io.axual.ksml.data.mapper.DataObjectFlattener;
import io.axual.ksml.data.mapper.DataTypeFlattener;
import io.axual.ksml.data.notation.NotationContext;
import io.axual.ksml.data.notation.binary.BinaryNotation;
import io.axual.ksml.data.notation.json.JsonNotation;
import io.axual.ksml.data.object.DataLong;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.definition.ParameterDefinition;
import io.axual.ksml.definition.PunctuatorDefinition;
import io.axual.ksml.definition.PythonSource;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.exception.TopologyException;
import io.axual.ksml.execution.ExecutionContext;
import io.axual.ksml.proxy.context.ProcessorContextProxy;
import io.axual.ksml.python.PythonContext;
import io.axual.ksml.python.PythonContextConfig;
import io.axual.ksml.python.PythonFunction;
import io.axual.ksml.type.UserType;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;

import static io.axual.ksml.user.UserTestSupport.tags;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class UserPunctuatorTest {
    private static final ProcessorContextProxy.Target NEXT = new ProcessorContextProxy.Target("main", new UserType(DataString.DATATYPE), new UserType(DataLong.DATATYPE));
    private static PythonContext pythonContext;

    @Mock
    private ProcessorContext<Object, Object> context;

    @BeforeAll
    static void setUp() {
        final var jsonNotation = new JsonNotation(new NotationContext(new DataObjectFlattener(), new DataTypeFlattener()));
        ExecutionContext.INSTANCE.notationLibrary().register(UserType.DEFAULT_NOTATION,
                new BinaryNotation(new NotationContext(new DataObjectFlattener(), new DataTypeFlattener()), jsonNotation::serde));
        pythonContext = new PythonContext(PythonContextConfig.builder().build());
    }

    @AfterAll
    static void tearDown() {
        pythonContext.close();
    }

    private static UserPunctuator punctuator(String... code) {
        final var function = FunctionDefinition.as(KSMLDSL.Functions.TYPE_PUNCTUATOR, "emit", new ParameterDefinition[0], PythonSource.of(new String[0], code, new String[0]), null, null, null);
        final var definition = new PunctuatorDefinition(function).withInjectedParameter(ProcessorContextProxy.INJECT_CONTEXT);
        return new UserPunctuator(PythonFunction.forFunction(pythonContext, "test", "emit", definition), tags());
    }

    private ProcessorContextProxy proxy() {
        return new ProcessorContextProxy(context, 1000L, NEXT, Map.of());
    }

    @Test
    @DisplayName("the punctuator gets the timestamp and the context, and forwards records in call order")
    void forwards() {
        final var punctuator = punctuator("context.forward('a', timestamp)", "context.forward('b', timestamp + 1)");
        punctuator.apply(null, proxy(), 1000L);
        @SuppressWarnings("unchecked") final ArgumentCaptor<Record<Object, Object>> captor = ArgumentCaptor.forClass(Record.class);
        verify(context, times(2)).forward(captor.capture(), eq("main"));
        assertThat(captor.getAllValues()).extracting(Record::key, Record::value)
                .containsExactly(
                        tuple(new DataString("a"), new DataLong(1000L)),
                        tuple(new DataString("b"), new DataLong(1001L)));
        assertThat(punctuator.warnedAboutResult()).isFalse();
    }

    @Test
    @DisplayName("a returned value is not forwarded, and a warning is logged once")
    void returnedValue() {
        final var punctuator = punctuator("return ('a', 1)");
        punctuator.apply(null, proxy(), 1000L);
        punctuator.apply(null, proxy(), 2000L);
        assertThat(punctuator.warnedAboutResult()).isTrue();
        verifyNoInteractions(context);
    }

    @Test
    @DisplayName("an exception in the punctuator is thrown, and records forwarded before it stay forwarded")
    void raises() {
        final var punctuator = punctuator("context.forward('a', 1)", "raise ValueError('boom')");
        final var proxy = proxy();
        assertThatThrownBy(() -> punctuator.apply(null, proxy, 1000L))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("ValueError: boom");
        verify(context).forward(any(Record.class), eq("main"));
    }

    @Test
    @DisplayName("a function with more than two required parameters can not be a punctuator")
    void tooManyParameters() {
        final var function = new UserFunction("ns", "fn", UserTestSupport.params(3), null, null, (String[]) null);
        final var tags = tags();
        assertThatThrownBy(() -> new UserPunctuator(function, tags)).isInstanceOf(TopologyException.class);
    }
}
