package io.axual.ksml.testrunner;

/*-
 * ========================LICENSE_START=================================
 * KSML Test Runner
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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for enriching record components and types with JSON Schema metadata.
 * Used by {@link TestDefinitionSchemaGenerator} to produce a JSON Schema for test
 * definition YAML files.
 *
 * <p>Per-component fields ({@code description}, {@code defaultValue}, {@code examples},
 * {@code required}, {@code minProperties}, {@code yamlName}) describe a single record
 * component. Per-type fields ({@code oneOfRequired}, {@code anyOfRequired}) describe a
 * structural constraint on the whole record/type.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.RECORD_COMPONENT, ElementType.TYPE})
public @interface JsonSchema {
    /** Human-readable description, emitted as the JSON Schema {@code description}. */
    String description() default "";

    /** Default value, emitted as the JSON Schema {@code default}. */
    String defaultValue() default "";

    /** Example values, emitted as the JSON Schema {@code examples} array. */
    String[] examples() default {};

    /** Whether this component is required. */
    boolean required() default false;

    /**
     * For Map-typed components, the minimum number of entries (emitted as
     * {@code minProperties}). Ignored on non-map components.
     */
    int minProperties() default 0;

    /**
     * Override the YAML field name for this component. Useful when the Java component name
     * collides with a Java keyword (e.g., {@code assertions} maps to YAML {@code assert}).
     */
    String yamlName() default "";

    /**
     * Type-level constraint: each named field becomes a {@code oneOf} variant requiring
     * exactly that field. Use to express "exactly one of X or Y must be present".
     */
    String[] oneOfRequired() default {};

    /**
     * Type-level constraint: each named field becomes an {@code anyOf} variant requiring
     * at least that field. Use to express "at least one of X or Y must be present".
     */
    String[] anyOfRequired() default {};
}
