package io.axual.ksml.execution;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2024 Axual B.V.
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

import io.axual.ksml.data.notation.Notation;
import io.axual.ksml.type.UserType;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Snapshots the shared {@code DEFAULT_NOTATION} entry of the process-wide
 * {@link ExecutionContext#notationLibrary()} before each test and restores it afterwards, so a test
 * that registers a (mock) default notation cannot leak it into other test classes or make the suite
 * order dependent.
 *
 * <p>Applied only to the operation unit tests that register a mock default notation, by extending
 * {@code OperationTestBase} (which carries {@code @ExtendWith(DefaultNotationIsolationExtension.class)}).
 * It is a no-op for any test that never touches the default notation.
 *
 * <p>The notation library is a JVM-wide singleton, so this guards sequential execution only. Parallel
 * test execution must stay off for this module.
 */
public class DefaultNotationIsolationExtension implements BeforeEachCallback, AfterEachCallback {
    private Notation previous;

    @Override
    public void beforeEach(ExtensionContext context) {
        previous = ExecutionContext.INSTANCE.notationLibrary().getIfExists(UserType.DEFAULT_NOTATION);
    }

    @Override
    public void afterEach(ExtensionContext context) {
        if (previous != null) {
            ExecutionContext.INSTANCE.notationLibrary().register(UserType.DEFAULT_NOTATION, previous);
        } else {
            ExecutionContext.INSTANCE.notationLibrary().unregister(UserType.DEFAULT_NOTATION);
        }
    }
}
