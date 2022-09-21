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


import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.Windowed;

import io.axual.ksml.data.type.WindowedType;
import io.axual.ksml.exception.KSMLTopologyException;
import io.axual.ksml.stream.KTableWrapper;
import io.axual.ksml.stream.StreamWrapper;

public class SuppressOperation extends BaseOperation {
    private final Suppressed<Windowed> suppressedWindowed;
    private final Suppressed<Object> suppressed;

    private SuppressOperation(OperationConfig config, Suppressed<Object> suppressed, Suppressed<Windowed> suppressedWindowed) {
        super(config);
        this.suppressed = suppressed != null ? suppressed.withName(name) : null;
        this.suppressedWindowed = suppressedWindowed != null ? suppressedWindowed.withName(name) : null;
    }

    public static SuppressOperation create(OperationConfig config, Suppressed<Object> suppressed) {
        return new SuppressOperation(config, suppressed, null);
    }

    public static SuppressOperation createWindowed(OperationConfig config, Suppressed<Windowed> suppressed) {
        return new SuppressOperation(config, null, suppressed);
    }

    @Override
    public StreamWrapper apply(KTableWrapper input) {
        if (suppressed != null) {
            return new KTableWrapper(input.table.suppress(suppressed), input.keyType(), input.valueType());
        }

        // Because of type erasure, we can not rely on Java to perform type checking the key
        // for us. Therefore, we check the type manually to ensure the user is applying the
        // "untilWindowCloses" suppression on the right KTable key type.

        // Validate that the key type is windowed
        if (input.keyType().type() instanceof WindowedType) {
            return new KTableWrapper(input.table.suppress((Suppressed) suppressedWindowed), input.keyType(), input.valueType());
        }
        // Throw an exception if the stream key type is not Windowed
        throw new KSMLTopologyException("Can not apply suppress operation to a KTable with key type " + input.keyType().type());
    }
}
