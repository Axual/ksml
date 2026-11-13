package io.axual.ksml.definition;

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

// Formally this is not a Definition in the sense that it does not show up in the KSML Language
// Specification. Therefore, it does not have the Definition suffix in the name. It does belong
// in this package because it is a crucial element during the generation of Python code for
// parameters that do not implement the DataObject interface.
public record InjectedParameter(
        String name,
        String proxyClassName,
        String proxyClassCode) {
}
