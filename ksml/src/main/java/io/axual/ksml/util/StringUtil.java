package io.axual.ksml.util;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 Axual B.V.
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
 * Class with String helper methods.
 */
public class StringUtil {
    private StringUtil() {
    }

    /**
     * Join string with a given separator.
     *
     * @param separator the separator
     * @param strings   the strings to join
     * @return the joined string
     */
    public static String join(String separator, String... strings) {
        StringBuilder builder = new StringBuilder();
        boolean first = true;
        for (String str : strings) {
            if (!first) {
                builder.append(separator);
            }
            builder.append(str);
            first = false;
        }

        return builder.toString();
    }
}
