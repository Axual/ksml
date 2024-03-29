package io.axual.ksml.util;

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

import java.util.ArrayList;
import java.util.List;

public class ListUtil {
    public static <T> List<T> union(List<T> list1, List<T> list2) {
        final var result = new ArrayList<T>(list1.size() + list2.size());
        result.addAll(list1);
        result.addAll(list2);
        return result;
    }

    public static <T> List<T> union(List<T> list1, List<T> list2, List<T> list3) {
        final var result = new ArrayList<T>(list1.size() + list2.size() + list3.size());
        result.addAll(list1);
        result.addAll(list2);
        result.addAll(list3);
        return result;
    }
}
