package io.axual.ksml.user;

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


import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.data.type.UnionType;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.metric.MetricTags;
import io.axual.ksml.python.Invoker;
import io.axual.ksml.type.UserTupleType;
import io.axual.ksml.type.UserType;

public class UserGenerator extends Invoker {
    public static final DataType EXPECTED_RESULT_TYPE = new UnionType(
            new UnionType.Member("singleMessage", new UserTupleType(UserType.UNKNOWN, UserType.UNKNOWN), "Single message", 1),
            new UnionType.Member("listOfMessages", new ListType(new UserTupleType(UserType.UNKNOWN, UserType.UNKNOWN)), "List of messages", 2));

    public UserGenerator(UserFunction function, MetricTags tags) {
        super(function, tags, KSMLDSL.Functions.TYPE_GENERATOR);
        verifyParameterCount(0);
        verifyResultType(EXPECTED_RESULT_TYPE);
    }

    public DataObject apply() {
        return timeExecutionOf(function::call);
    }
}
