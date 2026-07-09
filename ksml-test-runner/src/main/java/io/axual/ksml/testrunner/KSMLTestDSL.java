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

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.regex.Pattern;

/**
 * Single source of truth for the YAML field names of the KSML test-runner test-definition
 * format. The nesting mirrors the YAML object graph: top-level fields live on the outer
 * class, and one nested static class exists per YAML object ({@link Streams}, {@link Tests},
 * {@link Produce}, {@link Assert}, {@link Message}).
 *
 * <p>This class mirrors the design of {@code io.axual.ksml.dsl.KSMLDSL} in the {@code ksml}
 * module, which serves the same role for the KSML pipeline DSL.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class KSMLTestDSL {

    public static final String NAME = "name";
    public static final String DEFINITION = "definition";
    public static final String SCHEMA_DIRECTORY = "schemaDirectory";
    public static final String MODULE_DIRECTORY = "moduleDirectory";
    public static final String STREAMS = "streams";
    public static final String TESTS = "tests";

    /**
     * Identifier regex enforced for stream-map keys and test-map keys. Shared by the
     * parser (as the compiled {@link #IDENTIFIER_PATTERN}) and the schema generator
     * (as the source string in {@code patternProperties}).
     */
    public static final String IDENTIFIER_REGEX = "^[a-zA-Z]\\w*$";

    /** Compiled form of {@link #IDENTIFIER_REGEX}, for parser-side validation. */
    public static final Pattern IDENTIFIER_PATTERN = Pattern.compile(IDENTIFIER_REGEX);

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class Streams {
        public static final String TOPIC = "topic";
        public static final String KEY_TYPE = "keyType";
        public static final String VALUE_TYPE = "valueType";
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class Tests {
        public static final String DESCRIPTION = "description";
        public static final String PRODUCE = "produce";
        public static final String ASSERT = "assert";
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class Produce {
        public static final String TO = "to";
        public static final String MESSAGES = "messages";
        public static final String GENERATOR = "generator";
        public static final String COUNT = "count";

        /**
         * Field names of the function definition embedded under {@code produce.generator:}.
         * These spellings overlap with {@code io.axual.ksml.dsl.KSMLDSL.Functions} but are
         * deliberately duplicated here: the test-runner YAML is its own DSL and must not take
         * a compile-time dependency on the pipeline DSL's constants.
         */
        @NoArgsConstructor(access = AccessLevel.PRIVATE)
        public static final class Generator {
            public static final String NAME = "name";
            public static final String GLOBAL_CODE = "globalCode";
            public static final String CODE = "code";
            public static final String EXPRESSION = "expression";
        }
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class Assert {
        public static final String ON = "on";
        public static final String STORES = "stores";
        public static final String CODE = "code";
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class Message {
        public static final String KEY = "key";
        public static final String VALUE = "value";
        public static final String TIMESTAMP = "timestamp";
    }
}
