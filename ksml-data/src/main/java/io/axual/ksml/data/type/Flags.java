package io.axual.ksml.data.type;

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

import lombok.Getter;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * A simple, immutable-style holder for string-based feature flags that influence comparison behavior.
 *
 * <p>Flags are used throughout KSML (for example with deep-equality checks) to conditionally
 * ignore or include certain attributes in comparisons. Instances can be created empty, from a
 * set of names, or from a varargs of flag names.</p>
 */
@Getter
public class Flags {
    /**
     * An empty, reusable instance representing no flags being set.
     */
    public static final Flags EMPTY = new Flags();
    /**
     * The internal set of flag names that are considered enabled.
     */
    private final Set<String> flagSet = new HashSet<>();

    /**
     * Create an instance with no flags set.
     */
    public Flags() {
    }

    /**
     * Create an instance with the specified flag names set.
     *
     * @param flags flag names to enable
     */
    public Flags(String... flags) {
        flagSet.addAll(Arrays.asList(flags));
    }

    /**
     * Create an instance with flags initialized from the provided set.
     *
     * @param flagSet initial set of flags to enable
     */
    public Flags(Set<String> flagSet) {
        this.flagSet.addAll(flagSet);
    }

    /**
     * Check whether a given flag name is enabled.
     *
     * @param flag the flag name to query
     * @return true if the flag is present; false otherwise
     */
    public boolean isSet(String flag) {
        return flagSet.contains(flag);
    }
}
