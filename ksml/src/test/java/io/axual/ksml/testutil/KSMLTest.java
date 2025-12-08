package io.axual.ksml.testutil;

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

import org.junit.jupiter.api.Test;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation used to mark a test method as a KSML topology test.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Test
public @interface KSMLTest {

    String NO_SCHEMAS = "";
    String NO_MODULES = "";

    /** Classpath relative reference to the pipeline definition under test. */
    String topology();

    /** Optional classpath relative reference to a directory with AVRO schema definitions. */
    String schemaDirectory() default NO_SCHEMAS;

    /** Optional classpath relative reference to a directory with KSML modules. */
    String modulesDirectory() default NO_MODULES;

    /** Optional boolean to allow KSML host file access; default false. */
    boolean allowHostFileAccess() default false;

}
