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

import io.axual.ksml.data.object.DataObjectFlag;
import io.axual.ksml.data.schema.DataSchemaFlag;
import io.axual.ksml.data.type.DataTypeFlag;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

/**
 * A high-performance, immutable-style holder for feature flags that influence comparison behavior.
 *
 * <p>Flags are used throughout KSML (for example with deep-equality checks) to conditionally
 * ignore or include certain attributes in comparisons. Instances can be created empty, from a
 * set of flags, or from a varargs of flag values.</p>
 *
 * <p>Internally uses {@link EnumSet} for optimal performance when checking flag membership.</p>
 */
public class EqualityFlags {
    /**
     * An empty, reusable instance representing no flags being set.
     */
    public static final EqualityFlags EMPTY = new EqualityFlags();

    /**
     * EnumSet for DataObjectFlag instances.
     */
    private final EnumSet<DataObjectFlag> dataObjectFlags = EnumSet.noneOf(DataObjectFlag.class);

    /**
     * EnumSet for DataSchemaFlag instances.
     */
    private final EnumSet<DataSchemaFlag> dataSchemaFlags = EnumSet.noneOf(DataSchemaFlag.class);

    /**
     * EnumSet for DataTypeFlag instances.
     */
    private final EnumSet<DataTypeFlag> dataTypeFlags = EnumSet.noneOf(DataTypeFlag.class);

    /**
     * Create an instance with no flags set.
     */
    public EqualityFlags() {
    }

    /**
     * Create an instance with the specified flags set.
     *
     * @param flags flag values to enable
     */
    public EqualityFlags(EqualityFlag... flags) {
        for (EqualityFlag flag : flags) addFlag(flag);
    }

    /**
     * Create an instance with flags initialized from the provided set.
     *
     * @param flagSet initial set of flags to enable
     */
    public EqualityFlags(Set<EqualityFlag> flagSet) {
        flagSet.forEach(this::addFlag);
    }

    /**
     * Internal helper to add a flag to the appropriate EnumSet.
     * Uses pattern matching to route flags to the correct EnumSet.
     *
     * @param flag the flag to add
     */
    private void addFlag(EqualityFlag flag) {
        switch (flag) {
            case DataObjectFlag dof -> dataObjectFlags.add(dof);
            case DataSchemaFlag dsf -> dataSchemaFlags.add(dsf);
            case DataTypeFlag dtf -> dataTypeFlags.add(dtf);
            case null -> throw new IllegalArgumentException("Flag cannot be null");
            default -> throw new IllegalArgumentException("Unknown flag type: " + flag.getClass());
        }
    }

    /**
     * Check whether a given flag is enabled.
     * Uses pattern matching to check the appropriate EnumSet.
     *
     * @param flag the flag to query
     * @return true if the flag is present; false otherwise
     */
    public boolean isSet(EqualityFlag flag) {
        if (flag == null) return false;

        return switch (flag) {
            case DataObjectFlag dof -> dataObjectFlags.contains(dof);
            case DataSchemaFlag dsf -> dataSchemaFlags.contains(dsf);
            case DataTypeFlag dtf -> dataTypeFlags.contains(dtf);
            default -> throw new IllegalStateException("Unexpected flag type: " + flag.getClass());
        };
    }

    /**
     * Returns a view of all flags as a combined set.
     * This is primarily for compatibility and debugging purposes.
     *
     * @return a set containing all enabled flags
     */
    public Set<EqualityFlag> getAll() {
        Set<EqualityFlag> combined = new HashSet<>();
        combined.addAll(dataObjectFlags);
        combined.addAll(dataSchemaFlags);
        combined.addAll(dataTypeFlags);
        return combined;
    }
}
