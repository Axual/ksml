package io.axual.ksml.operation;

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

import io.axual.ksml.execution.DefaultNotationIsolationExtension;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Base for operation unit tests that register a mock default notation through
 * {@link OperationTestSupport#mockContext()}. It applies {@link DefaultNotationIsolationExtension}
 * so that mock is snapshotted and restored around each test and cannot leak into other test classes.
 *
 * <p>Scoped deliberately to these tests via inheritance rather than a module-wide extension
 * auto-detection switch, so the blast radius stays exactly the operation tests that need it.
 */
@ExtendWith(DefaultNotationIsolationExtension.class)
abstract class OperationTestBase {
}
