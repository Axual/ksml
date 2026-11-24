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

import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataLong;
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.definition.InjectedParameter;
import io.axual.ksml.definition.ParameterDefinition;
import io.axual.ksml.definition.PythonSource;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.exception.TopologyException;
import io.axual.ksml.type.UserType;
import io.axual.ksml.user.UserFunction;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PythonFunctionTest {
    @BeforeAll
    static void warmupGraalVM() {
        // The very first Python context creation in a JVM can fail with a libgraal cold-start
        // error ("Error decoding exception: [B@..."). Build one throwaway context here so the
        // field initializer below always runs against a warm libgraal isolate.
        try {
            new PythonContext(PythonContextConfig.builder().build());
        } catch (Exception _) {
            // Warmup only — the real assertions happen in the @Test methods.
        }
    }

    final PythonContext context = new PythonContext(PythonContextConfig.builder().build());
    final ParameterDefinition one = new ParameterDefinition("one", DataInteger.DATATYPE);
    final ParameterDefinition two = new ParameterDefinition("two", DataInteger.DATATYPE);
    final ParameterDefinition[] params = new ParameterDefinition[]{one, two};
    final UserType resultType = new UserType(UserType.DEFAULT_NOTATION, DataInteger.DATATYPE);

    @ParameterizedTest
    @CsvSource({"1, 2, 3", "100,100,200", "100, -1, 99", "99, -100, -1"})
    void testAdditionExpression(Integer i1, Integer i2, Integer sum) {
        final var adderDef = FunctionDefinition.as(KSMLDSL.Functions.TYPE_GENERIC, "test", "adder", params, PythonSource.of(null, null, "one + two"), resultType, null, null);
        final var adder = PythonFunction.forFunction(context, adderDef);

        final var arg1 = new DataInteger(i1);
        final var arg2 = new DataInteger(i2);

        final var result = adder.call(arg1, arg2);
        assertThat(result).isInstanceOf(DataInteger.class);
        assertThat(((DataInteger) result).value()).isEqualTo(sum);
    }

    /**
     * Test creating a Python function and calling it in the expression.
     */
    @ParameterizedTest
    @CsvSource({"1, 2, 3", "100,100,200", "100, -1, 99", "99, -100, -1"})
    void testAdditionCode(Integer i1, Integer i2, Integer sum) {
        var pythonCode = """
                def myAddFunc(one, two):
                  return one + two
                
                """;
        final var adderDef = FunctionDefinition.as(KSMLDSL.Functions.TYPE_GENERIC, "test", "adder", params, PythonSource.of(null, pythonCode.split("\n"), new String[]{"myAddFunc(one, two)"}), resultType, null, null);
        final var adder = PythonFunction.forFunction(context, adderDef);

        final var arg1 = new DataInteger(i1);
        final var arg2 = new DataInteger(i2);

        final var result = adder.call(arg1, arg2);
        assertThat(result).isInstanceOf(DataInteger.class);
        assertThat(((DataInteger) result).value()).isEqualTo(sum);
    }

    /**
     * Test creating a Python function in the global code and calling it in the expression.
     */
    @ParameterizedTest
    @CsvSource({"1, 2, 3", "100,100,200", "100, -1, 99", "99, -100, -1"})
    void testAdditionGlobalCode(Integer i1, Integer i2, Integer sum) {
        var pythonCode = """
                def myAddFunc(one, two):
                  return one + two
                
                """;
        final var adderDef = FunctionDefinition.as(KSMLDSL.Functions.TYPE_GENERIC, "test", "adder", params, PythonSource.of(pythonCode.split("\n"), null, new String[]{"myAddFunc(one, two)"}), resultType, null, null);
        final var adder = PythonFunction.forFunction(context, adderDef);

        final var arg1 = new DataInteger(i1);
        final var arg2 = new DataInteger(i2);

        final var result = adder.call(arg1, arg2);
        assertThat(result).isInstanceOf(DataInteger.class);
        assertThat(((DataInteger) result).value()).isEqualTo(sum);
    }

    @Test
    /*
      Test that Null Key/Values are accepted as parameters
     */
    void testNullKeyValue() {
        final var stringResultType = new UserType(UserType.DEFAULT_NOTATION, DataString.DATATYPE);
        final var concatDef = FunctionDefinition.as(KSMLDSL.Functions.TYPE_GENERIC, "test", "concat", params, PythonSource.of(null, null, "str(one is None) + ' ' + str(two is None)"), stringResultType, null, null);
        final var concat = PythonFunction.forFunction(context, concatDef);

        final var nullArg = DataNull.INSTANCE;
        final var nonNullArg = new DataInteger(1);

        final var expectedResultNullKey = "True False";
        var resultNullKey = concat.call(nullArg, nonNullArg);
        assertThat(resultNullKey).isInstanceOf(DataString.class);
        assertThat(((DataString) resultNullKey).value()).isEqualTo(expectedResultNullKey);

        final var expectedResultNullValue = "False True";
        var resultNullValue = concat.call(nonNullArg, nullArg);
        assertThat(resultNullValue).isInstanceOf(DataString.class);
        assertThat(((DataString) resultNullValue).value()).isEqualTo(expectedResultNullValue);

        final var expectedResultNullKeyValue = "True True";
        var resultNullKeyValue = concat.call(nullArg, nullArg);
        assertThat(resultNullKeyValue).isInstanceOf(DataString.class);
        assertThat(((DataString) resultNullKeyValue).value()).isEqualTo(expectedResultNullKeyValue);

    }

    @Test
    void injectedVariablesDefaultToNoneAndUnknownNamesAreRejected() {
        final var stringType = new UserType(UserType.DEFAULT_NOTATION, DataString.DATATYPE);
        final var definition = FunctionDefinition.as(KSMLDSL.Functions.TYPE_GENERIC, "test", "injected", new ParameterDefinition[0], PythonSource.of(null, null, "str(context)"), stringType, java.util.List.of(io.axual.ksml.proxy.context.ProcessorContextProxy.INJECT_CONTEXT), null);
        final var function = PythonFunction.forFunction(context, definition);

        assertThat(function.call()).isEqualTo(new DataString("None"));
        // An injected variable is set during the call only; a later call that does not inject it does not see it
        final var storesDefinition = FunctionDefinition.as(KSMLDSL.Functions.TYPE_GENERIC, "test", "injectedStores", new ParameterDefinition[0], PythonSource.of(null, null, "str(type(stores).__name__)"), stringType, java.util.List.of(new io.axual.ksml.definition.InjectedParameter(io.axual.ksml.user.UserFunction.INJECT_STORES)), null);
        final var storesFunction = PythonFunction.forFunction(context, storesDefinition);
        assertThat(storesFunction.call(storesFunction.withStores(new io.axual.ksml.store.StateStores()))).isEqualTo(new DataString("dict"));
        assertThat(storesFunction.call()).isEqualTo(new DataString("NoneType"));
        assertThatThrownBy(() -> function.call(java.util.Map.of("unknown", 1)))
                .isInstanceOf(io.axual.ksml.exception.ExecutionException.class)
                .hasMessageContaining("can not inject unknown variable 'unknown'");
        // A function only gets the variables it declares
        assertThatThrownBy(() -> storesFunction.call(java.util.Map.of(io.axual.ksml.proxy.context.ProcessorContextProxy.INJECT_CONTEXT.name(), 1)))
                .isInstanceOf(io.axual.ksml.exception.ExecutionException.class)
                .hasMessageContaining("can not inject unknown variable 'context'");
        assertThat(function.withStores(new io.axual.ksml.store.StateStores())).isEmpty();
    }

    @Test
    void returnsFloatsWithAFraction() {
        final var doubleType = new UserType(UserType.DEFAULT_NOTATION, io.axual.ksml.data.object.DataDouble.DATATYPE);
        final var definition = FunctionDefinition.as(KSMLDSL.Functions.TYPE_GENERIC, "test", "half", new ParameterDefinition[0], PythonSource.of(null, null, "1.5"), doubleType, null, null);
        final var function = PythonFunction.forFunction(context, definition);
        assertThat(function.call()).isEqualTo(new io.axual.ksml.data.object.DataDouble(1.5));
    }

    @Test
    void injectedValuesStayWithTheirThread() {
        final var definition = FunctionDefinition.as(
                KSMLDSL.Functions.TYPE_GENERIC,
                "test",
                "probe",
                new ParameterDefinition[0],
                PythonSource.of(null, null, "stores[\"id\"]"),
                new UserType(UserType.DEFAULT_NOTATION, DataLong.DATATYPE),
                List.of(new InjectedParameter(UserFunction.INJECT_STORES)),
                null);
        final var function = PythonFunction.forFunction(context, definition);
        final var wrong = new AtomicLong();
        try (var pool = Executors.newFixedThreadPool(4)) {
            for (long id = 0; id < 4; id++) {
                final var injections = Map.<String, Object>of(UserFunction.INJECT_STORES, Map.of("id", id));
                final var expected = new DataLong(id);
                pool.submit(() -> {
                    for (int i = 0; i < 50_000; i++) {
                        try {
                            if (!expected.equals(function.call(injections))) wrong.incrementAndGet();
                        } catch (RuntimeException e) {
                            wrong.incrementAndGet();
                        }
                    }
                });
            }
        }
        assertThat(wrong.get()).isZero();
    }

    private PythonFunction clashFunction(ParameterDefinition[] parameters, List<InjectedParameter> injected, List<String> storeNames) {
        final var definition = FunctionDefinition.as(KSMLDSL.Functions.TYPE_GENERIC, "test", "clash", parameters, PythonSource.of(null, null, "None"), null, injected, storeNames);
        return PythonFunction.forFunction(context, definition);
    }

    @Test
    void distinctLocalVariableNamesAreAccepted() {
        final var function = clashFunction(params, List.of(new InjectedParameter("context")), List.of("totals", "counts", "totals"));
        assertThat(function.definition().injectedParameters()).extracting(InjectedParameter::name).containsExactly("context", UserFunction.INJECT_STORES);
    }

    @Test
    void storeWithNameOfInjectedVariableIsRejected() {
        assertThatThrownBy(() -> clashFunction(params, List.of(new InjectedParameter("context")), List.of("context")))
                .isInstanceOf(TopologyException.class)
                .hasMessageEndingWith("The store \"context\" of function test.clash clashes with the injected variable of the same name, rename the store");
    }

    @Test
    void storeNamedStoresIsRejected() {
        // The stores variable is injected for every function that uses stores
        assertThatThrownBy(() -> clashFunction(params, List.of(), List.of(UserFunction.INJECT_STORES)))
                .isInstanceOf(TopologyException.class)
                .hasMessageContaining("The store \"stores\" of function test.clash clashes with the injected variable of the same name");
    }

    @Test
    void storeWithNameOfParameterIsRejected() {
        assertThatThrownBy(() -> clashFunction(params, List.of(), List.of("two")))
                .isInstanceOf(TopologyException.class)
                .hasMessageContaining("The store \"two\" of function test.clash clashes with the parameter of the same name");
    }

    @Test
    void injectedVariableWithNameOfParameterIsRejected() {
        assertThatThrownBy(() -> clashFunction(params, List.of(new InjectedParameter("one")), List.of()))
                .isInstanceOf(TopologyException.class)
                .hasMessageContaining("The injected variable \"one\" of function test.clash clashes with the parameter of the same name");
    }

    @Test
    void storeMayShadowLog() {
        // log and metrics are builtins, so a store with the same name hides them only inside the function
        final var function = clashFunction(params, List.of(), List.of("log"));
        assertThat(function.definition().storeNames()).containsExactly("log");
    }

    private static final UserType STRING_TYPE = new UserType(UserType.DEFAULT_NOTATION, DataString.DATATYPE);

    private PythonFunction logFunction(PythonContext pythonContext, String name, String globalCode, String expression) {
        final var definition = FunctionDefinition.as(KSMLDSL.Functions.TYPE_GENERIC, "test", name, new ParameterDefinition[0], PythonSource.of(globalCode, null, expression), STRING_TYPE, null, null);
        return PythonFunction.forFunction(pythonContext, definition);
    }

    private static String loggerName(String name) {
        return "test." + KSMLDSL.Functions.TYPE_GENERIC + "." + name;
    }

    @Test
    void functionLogsUnderItsOwnName() {
        final var function = logFunction(context, "own_name", null, "log.getName()");
        assertThat(function.call()).isEqualTo(new DataString(loggerName("own_name")));
    }

    @Test
    void globalCodeHelperLogsUnderTheNameOfTheCallingFunction() {
        final var function = logFunction(context, "calls_helper", """
                def helper():
                  log.info("from the helper")
                  return log.getName()
                """, "helper()");
        assertThat(function.call()).isEqualTo(new DataString(loggerName("calls_helper")));
    }

    @Test
    void metricsIsAvailableInGlobalCodeHelpers() {
        final var function = logFunction(context, "metrics_helper", """
                def helper():
                  import ksml
                  return str(metrics is not None and metrics is ksml.metrics)
                """, "helper()");
        assertThat(function.call()).isEqualTo(new DataString("True"));
    }

    @Test
    void logOutsideAFunctionUsesTheKsmlLogger() {
        assertThat(context.context().eval("python", "import ksml\nksml.log.getName()").asString()).isEqualTo("ksml");
    }

    @Test
    void moduleFunctionLogsUnderTheNameOfTheCallingFunction(@TempDir Path moduleDir) throws IOException {
        Files.writeString(moduleDir.resolve("log_helpers.py"), """
                from ksml import log as imported_log

                def names():
                  return log.getName() + "|" + imported_log.getName()
                """);
        try (var moduleContext = new PythonContext(PythonContextConfig.builder().modulePath(moduleDir.toString()).build())) {
            final var function = logFunction(moduleContext, "calls_module", "import log_helpers", "log_helpers.names()");
            final var expected = loggerName("calls_module");
            assertThat(function.call()).isEqualTo(new DataString(expected + "|" + expected));
        }
    }

    @Test
    void helperLogsUnderTheCallingFunctionOnEachThread() {
        final var globalCode = """
                def shared_helper():
                  s = 0
                  for i in range(100): s += i
                  return log.getName()
                """;
        final var first = logFunction(context, "first_caller", globalCode, "shared_helper()");
        final var second = logFunction(context, "second_caller", globalCode, "shared_helper()");
        final var wrong = new AtomicLong();
        try (var pool = Executors.newFixedThreadPool(4)) {
            for (int thread = 0; thread < 4; thread++) {
                final var function = thread % 2 == 0 ? first : second;
                final var expected = new DataString(loggerName(thread % 2 == 0 ? "first_caller" : "second_caller"));
                pool.submit(() -> {
                    for (int i = 0; i < 2_000; i++) {
                        try {
                            if (!expected.equals(function.call())) wrong.incrementAndGet();
                        } catch (RuntimeException e) {
                            wrong.incrementAndGet();
                        }
                    }
                });
            }
        }
        assertThat(wrong.get()).isZero();
    }
}
