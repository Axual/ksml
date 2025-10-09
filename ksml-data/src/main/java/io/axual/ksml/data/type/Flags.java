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

@Getter
public class Flags {
    public static final Flags EMPTY = new Flags();
    private final Set<String> flagSet = new HashSet<>();

    public Flags() {
    }

    public Flags(String... flags) {
        flagSet.addAll(Arrays.asList(flags));
    }

    public Flags(Set<String> flagSet) {
        this.flagSet.addAll(flagSet);
    }

    public boolean isSet(String flag) {
        return flagSet.contains(flag);
    }
}
