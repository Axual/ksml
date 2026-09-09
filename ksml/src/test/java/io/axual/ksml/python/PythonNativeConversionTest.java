package io.axual.ksml.python;

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

import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataList;
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.definition.ParameterDefinition;
import io.axual.ksml.definition.PythonSource;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.type.UserType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Confirms key/value/aggregatedValue arrive in Python as genuine dict/list values, not proxies. */
class PythonNativeConversionTest {
    @BeforeAll
    static void warmupGraalVM() {
        try {
            new PythonContext(PythonContextConfig.builder().build());
        } catch (Exception _) {
            // Warmup only.
        }
    }

    final PythonContext context = new PythonContext(PythonContextConfig.builder().build());
    final ParameterDefinition valueParam = new ParameterDefinition("value", DataType.UNKNOWN);
    final UserType stringResultType = new UserType(UserType.DEFAULT_NOTATION, DataString.DATATYPE);

    private DataStruct nestedPayload() {
        var nested = new DataStruct();
        nested.put("tag", new DataString("hello"));
        var numbers = new DataList(DataInteger.DATATYPE);
        numbers.add(new DataInteger(1), new DataInteger(2), new DataInteger(3));
        var payload = new DataStruct();
        payload.put("day", new DataString("2026-01-01"));
        payload.put("nested", nested);
        payload.put("numbers", numbers);
        payload.put("owner", DataNull.INSTANCE);
        return payload;
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("nativeConversionChecks")
    void valuesPassedToPythonAreGenuineNativeTypes(String description, String code) {
        var def = FunctionDefinition.as(KSMLDSL.Functions.TYPE_GENERIC, "check", new ParameterDefinition[]{valueParam},
                PythonSource.of(null, code.split("\n"), new String[]{"'OK'"}), stringResultType, null);
        var fn = PythonFunction.forFunction(context, "test", "check", def);

        var result = fn.call(nestedPayload());
        assertThat(result).isInstanceOf(DataString.class);
        assertThat(((DataString) result).value()).isEqualTo("OK");
    }

    static Stream<Arguments> nativeConversionChecks() {
        return Stream.of(
                Arguments.of("value is a genuine native type, not a proxy", """
                        assert type(value) is dict, f"expected real dict, got {type(value)}"
                        assert type(value["nested"]) is dict, f"expected real nested dict, got {type(value['nested'])}"
                        assert type(value["numbers"]) is list, f"expected real list, got {type(value['numbers'])}"
                        assert value["owner"] is None, f"expected real None, got {value['owner']!r}"
                        'OK'
                        """),
                Arguments.of("deepcopy works on value", """
                        import copy
                        copy.deepcopy(value)
                        'OK'
                        """),
                // A null field must be a genuine None, not a foreign/interop null - deepcopy on a
                // dict/list only fails on the null field itself, so this needs its own check.
                Arguments.of("deepcopy works on a null field nested in value", """
                        import copy
                        backup = copy.deepcopy(value)
                        assert backup["owner"] is None
                        'OK'
                        """),
                // Confirms the billing app's exact pattern: dict(value) at the top level, then an
                // explicit copy of the nested part before mutating it, must not affect the original.
                Arguments.of("shallow copy of an explicit dict does not leak into the original", """
                        shallow = dict(value)
                        shallow["nested"] = dict(shallow["nested"])
                        shallow["nested"]["tag"] = "mutated"
                        assert value["nested"]["tag"] == "hello", "mutating an explicit copy must not change the original"
                        'OK'
                        """),
                // The exact style the billing app used before this fix (plain copy.deepcopy(), no
                // to_native() helper), confirming users do not need the workaround any more.
                Arguments.of("the original to_native() workaround is no longer needed", """
                        import copy
                        backup = copy.deepcopy(value)
                        backup["nested"]["tag"] = "changed"
                        assert value["nested"]["tag"] == "hello"
                        assert backup["nested"]["tag"] == "changed"
                        'OK'
                        """)
        );
    }
}
