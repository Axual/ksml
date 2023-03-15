package io.axual.ksml.python;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2022 Axual B.V.
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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataPrimitive;
import io.axual.ksml.data.type.UserType;
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.definition.ParameterDefinition;
import io.axual.ksml.notation.binary.BinaryNotation;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PythonFunctionTest {
    PythonContext context = new PythonContext(null);
    ParameterDefinition one = new ParameterDefinition("one", DataInteger.DATATYPE);
    ParameterDefinition two = new ParameterDefinition("two", DataInteger.DATATYPE);
    ParameterDefinition[] params = new ParameterDefinition[]{one, two};
    UserType resultType = new UserType(BinaryNotation.NOTATION_NAME, DataInteger.DATATYPE);

    /**
     * Test addition in Python expression.
     */
    @ParameterizedTest
    @CsvSource({"1, 2, 3", "100,100,200", "100, -1, 99", "99, -100, -1"})
    void testAdditionExpression(Integer i1, Integer i2, Integer sum) {
        FunctionDefinition adderDef = FunctionDefinition.as(params, resultType, "one + two", null, null);
        PythonFunction adder = new PythonFunction(context, "adder", adderDef);

        DataObject arg1 = new DataInteger(i1);
        DataObject arg2 = new DataInteger(i2);

        DataPrimitive result = (DataPrimitive) adder.call(arg1, arg2);
        assertEquals(sum, result.value());
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
        FunctionDefinition adderDef = FunctionDefinition.as(params, resultType, "myAddFunc(one, two)", pythonCode.split("\n"), null);
        PythonFunction adder = new PythonFunction(context, "adder", adderDef);

        DataObject arg1 = new DataInteger(i1);
        DataObject arg2 = new DataInteger(i2);

        DataPrimitive result = (DataPrimitive) adder.call(arg1, arg2);
        assertEquals(sum, result.value());
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
        FunctionDefinition adderDef = FunctionDefinition.as(params, resultType, "myAddFunc(one, two)", null, pythonCode.split("\n"));
        PythonFunction adder = new PythonFunction(context, "adder", adderDef);

        DataObject arg1 = new DataInteger(i1);
        DataObject arg2 = new DataInteger(i2);

        DataPrimitive result = (DataPrimitive) adder.call(arg1, arg2);
        assertEquals(sum, result.value());
    }
}
