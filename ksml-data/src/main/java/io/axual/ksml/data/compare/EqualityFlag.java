package io.axual.ksml.data.compare;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library
 * %%
 * Copyright (C) 2021 - 2025 Axual B.V.
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

/**
 * Marker interface for all comparison flag enums used throughout KSML data layer.
 *
 * <p>This interface is implemented by three specific enum types:
 * <ul>
 *   <li>{@link io.axual.ksml.data.type.DataTypeFlag} - flags for DataType comparisons</li>
 *   <li>{@link io.axual.ksml.data.schema.DataSchemaFlag} - flags for DataSchema comparisons</li>
 *   <li>{@link io.axual.ksml.data.object.DataObjectFlag} - flags for DataObject comparisons</li>
 * </ul>
 *
 * <p>Note: This interface cannot be sealed because the implementing enums are in different packages,
 * and sealed types can only permit subtypes in the same package.</p>
 */
public interface EqualityFlag {
}