package io.axual.ksml.operation;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 Axual B.V.
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


import org.apache.kafka.streams.kstream.Named;

import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.user.UserForeachAction;
import io.axual.ksml.user.UserFunction;

public class ForEachOperation extends BaseOperation {
    private static final String FOREACHACTION_NAME = "ForEachAction";
    private final UserFunction forEachAction;

    public ForEachOperation(OperationConfig config, UserFunction forEachAction) {
        super(config);
        this.forEachAction = forEachAction;
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input) {
        /*    Kafka Streams method signature:
         *    KTable<K, V> filterNot(
         *          final Predicate<? super K, ? super V> predicate
         *          final Named named)
         */

        var k = input.keyType().userType().dataType();
        var v = input.valueType().userType().dataType();
        checkFunction(FOREACHACTION_NAME, forEachAction, equalTo(DataNull.DATATYPE), superOf(k), superOf(v));

        input.stream.foreach(new UserForeachAction(forEachAction), Named.as(name));
        return null;
    }
}
