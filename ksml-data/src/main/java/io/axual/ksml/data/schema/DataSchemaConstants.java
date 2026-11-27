package io.axual.ksml.data.schema;

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

import java.util.HashSet;
import java.util.Set;

/**
 * Defines constants used by the data schema components in the KSML framework.
 * <p>
 * The {@code DataSchemaConstants} class serves as a centralized repository for constant values
 * associated with data schema processing and functionality within the application. These constants
 * aid in maintaining consistency and avoiding duplication of hardcoded values across the codebase.
 * </p>
 * <p>
 * This class is designed to be a utility class, and therefore it is not meant to be instantiated.
 * The constructor is private to enforce this restriction.
 * </p>
 */
public class DataSchemaConstants {
    /**
     * Constant value representing the absence of a tag.
     */
    public static final int NO_TAG = -1;

    /**
     * The default namespace used for data schemas in the KSML framework.
     * <p>
     * This namespace is primarily used to uniquely identify schema definitions
     * in the context of KSML's data schema processing.
     * </p>
     */
    public static final String DATA_SCHEMA_KSML_NAMESPACE = "io.axual.ksml.data";

    /**
     * The type names used to represent data schemas in the KSML framework.
     */
    public static final String ANY_TYPE = "any";
    public static final String NULL_TYPE = "null";
    public static final String BOOLEAN_TYPE = "boolean";
    public static final String BYTE_TYPE = "byte";
    public static final String SHORT_TYPE = "short";
    public static final String INTEGER_TYPE = "integer";
    public static final String LONG_TYPE = "long";
    public static final String FLOAT_TYPE = "float";
    public static final String DOUBLE_TYPE = "double";
    public static final String BYTES_TYPE = "bytes";
    public static final String FIXED_TYPE = "fixed";
    public static final String STRING_TYPE = "string";
    public static final String ENUM_TYPE = "enum";
    public static final String LIST_TYPE = "list";
    public static final String MAP_TYPE = "map";
    public static final String STRUCT_TYPE = "struct";
    public static final String TUPLE_TYPE = "tuple";
    public static final String UNION_TYPE = "union";

    /**
     * The set of all type names used to represent data schemas in the KSML framework.
     */
    private static final Set<String> TYPES = new HashSet<>();

    static {
        TYPES.add(ANY_TYPE);
        TYPES.add(NULL_TYPE);
        TYPES.add(BOOLEAN_TYPE);
        TYPES.add(BYTE_TYPE);
        TYPES.add(SHORT_TYPE);
        TYPES.add(INTEGER_TYPE);
        TYPES.add(LONG_TYPE);
        TYPES.add(FLOAT_TYPE);
        TYPES.add(DOUBLE_TYPE);
        TYPES.add(BYTES_TYPE);
        TYPES.add(FIXED_TYPE);
        TYPES.add(STRING_TYPE);
        TYPES.add(ENUM_TYPE);
        TYPES.add(LIST_TYPE);
        TYPES.add(MAP_TYPE);
        TYPES.add(STRUCT_TYPE);
        TYPES.add(TUPLE_TYPE);
        TYPES.add(UNION_TYPE);
    }

    /**
     * Private constructor to prevent instantiation of this utility class.
     * <p>
     * Since this class only defines constants and is not meant to hold state
     * or provide instance-level functionality, its constructor is intentionally private.
     * </p>
     */
    private DataSchemaConstants() {
        // Prevent instantiation.
    }

    public static boolean isType(String type) {
        return TYPES.contains(type);
    }
}
