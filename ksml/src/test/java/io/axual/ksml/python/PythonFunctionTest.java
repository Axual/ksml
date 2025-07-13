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
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.definition.ParameterDefinition;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.type.UserType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class PythonFunctionTest {
    final PythonContext context = new PythonContext(PythonContextConfig.builder().build());
    final ParameterDefinition one = new ParameterDefinition("one", DataInteger.DATATYPE);
    final ParameterDefinition two = new ParameterDefinition("two", DataInteger.DATATYPE);
    final ParameterDefinition[] params = new ParameterDefinition[]{one, two};
    final UserType resultType = new UserType(UserType.DEFAULT_NOTATION, DataInteger.DATATYPE);

    @ParameterizedTest
    @CsvSource({"1, 2, 3", "100,100,200", "100, -1, 99", "99, -100, -1"})
    void testAdditionExpression(Integer i1, Integer i2, Integer sum) {
        final var adderDef = FunctionDefinition.as(KSMLDSL.Functions.TYPE_GENERIC, "adder", params, null, null, "one + two", resultType, null);
        final var adder = PythonFunction.forFunction(context, "test", "adder", adderDef);

        final var arg1 = new DataInteger(i1);
        final var arg2 = new DataInteger(i2);

        final var result = adder.call(arg1, arg2);
        assertInstanceOf(DataInteger.class, result);
        assertEquals(sum, ((DataInteger) result).value());
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
        final var adderDef = FunctionDefinition.as(KSMLDSL.Functions.TYPE_GENERIC, "adder", params, null, pythonCode.split("\n"), new String[]{"myAddFunc(one, two)"}, resultType, null);
        final var adder = PythonFunction.forFunction(context, "test", "adder", adderDef);

        final var arg1 = new DataInteger(i1);
        final var arg2 = new DataInteger(i2);

        final var result = adder.call(arg1, arg2);
        assertInstanceOf(DataInteger.class, result);
        assertEquals(sum, ((DataInteger) result).value());
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
        final var adderDef = FunctionDefinition.as(KSMLDSL.Functions.TYPE_GENERIC, "adder", params, pythonCode.split("\n"), null, new String[]{"myAddFunc(one, two)"}, resultType, null);
        final var adder = PythonFunction.forFunction(context, "test", "adder", adderDef);

        final var arg1 = new DataInteger(i1);
        final var arg2 = new DataInteger(i2);

        final var result = adder.call(arg1, arg2);
        assertInstanceOf(DataInteger.class, result);
        assertEquals(sum, ((DataInteger) result).value());
    }

    @Test
    /*
      Test that Null Key/Values are accepted as parameters
     */
    void testNullKeyValue() {
        final var stringResultType = new UserType(UserType.DEFAULT_NOTATION, DataString.DATATYPE);
        final var concatDef = FunctionDefinition.as(KSMLDSL.Functions.TYPE_GENERIC, "concat", params, null, null, "str(one is None) + ' ' + str(two is None)", stringResultType, null);
        final var concat = PythonFunction.forFunction(context, "test", "adder", concatDef);

        final var nullArg = DataNull.INSTANCE;
        final var nonNullArg = new DataInteger(1);

        final var expectedResultNullKey = "True False";
        var resultNullKey = concat.call(nullArg, nonNullArg);
        assertInstanceOf(DataString.class, resultNullKey);
        assertEquals(expectedResultNullKey, ((DataString) resultNullKey).value());

        final var expectedResultNullValue = "False True";
        var resultNullValue = concat.call(nonNullArg, nullArg);
        assertInstanceOf(DataString.class, resultNullValue);
        assertEquals(expectedResultNullValue, ((DataString) resultNullValue).value());

        final var expectedResultNullKeyValue = "True True";
        var resultNullKeyValue = concat.call(nullArg, nullArg);
        assertInstanceOf(DataString.class, resultNullKeyValue);
        assertEquals(expectedResultNullKeyValue, ((DataString) resultNullKeyValue).value());

    }
}
