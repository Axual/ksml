package io.axual.ksml.client.resolving;

/*-
 * ========================LICENSE_START=================================
 * Extended Kafka clients for KSML
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

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.text.StringSubstitutor;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
@Slf4j
public class PatternResolver implements Resolver {
    private static final String ALPHANUM_CHARACTERS = "a-zA-Z0-9_";
    private static final String DASH_CHARACTER = "-";
    private static final String DOT_CHARACTER = ".";
    private static final String LITERAL_CHARACTERS = "$#-";
    private static final String LITERAL_REGEX = characterRegex(LITERAL_CHARACTERS, true);
    private static final String FIELD_NAME_CHARACTERS = ALPHANUM_CHARACTERS + DOT_CHARACTER;
    private static final String FIELD_VALUE_CHARACTERS = ALPHANUM_CHARACTERS + DOT_CHARACTER;
    private static final String FIELD_VALUE_REGEX = characterRegex(FIELD_VALUE_CHARACTERS, true);
    private static final String DEFAULT_FIELD_VALUE_CHARACTERS = FIELD_VALUE_CHARACTERS + DASH_CHARACTER;
    private static final String DEFAULT_FIELD_VALUE_REGEX = characterRegex(DEFAULT_FIELD_VALUE_CHARACTERS, true);
    private static final String FIELD_NAME_PREFIX = "{";
    private static final String FIELD_NAME_SUFFIX = "}";
    private static final String FIELD_NAME_REGEX = escape(FIELD_NAME_PREFIX) + characterRegex(FIELD_NAME_CHARACTERS, true) + escape(FIELD_NAME_SUFFIX);
    public static final String FIELD_NAME_OR_LITERAL_MATCH_REGEX = "(" + FIELD_NAME_REGEX + "|" + LITERAL_REGEX + ")";
    private static final Pattern FIELD_NAME_OR_LITERAL_PATTERN = Pattern.compile(FIELD_NAME_OR_LITERAL_MATCH_REGEX);
    private final Map<String, String> defaultFieldValues;
    protected final String defaultFieldName;
    private final List<String> fields;
    private final String resolvePattern;
    private final Pattern unresolvePattern;

    @Builder
    private record PatternParseResult(String resolvePattern, Pattern unresolvePattern, List<String> fields) {
    }

    /**
     * Constructs the PatternContextConverter for a specific pattern. The defaultPlaceholderValue
     * is the field name for the resource type
     *
     * <p>A pattern definition is a string with delimited field names that are used to build to and
     * from the context map.
     * The final field name should be for the target resource type, which
     * <br/>The following pattern is for a Kafka topic and has three fields, tenant, instance, environment, which are separated by two hyphens<br/>
     * <pre>{tenant}--{instance}--{environment}--{topic}</pre></p>
     * <p>The following pattern is functionally the same as the previous pattern<br/>
     * <pre>{tenant}--{instance}--{environment}--</pre></p>
     * <p>
     * To construct the above pattern the constructor call looks like this
     * <pre>{@code
     * PatternContextConverter first("tenant}--{instance}--{environment}--{topic}", "topic");
     * PatternContextConverter first("tenant}--{instance}--{environment}--", "topic");
     * }</pre>
     *
     * @param pattern          the pattern to use
     * @param defaultFieldName the resource type that the pattern should end with
     * @throws IllegalArgumentException thrown when a parameter is null, if pattern is invalid or if the defaultPlaceholderValue is empty
     */
    public PatternResolver(final String pattern, final String defaultFieldName, Map<String, String> defaultFieldValues) {
        if (pattern == null) {
            throw new IllegalArgumentException("pattern cannot be null");
        }

        if (defaultFieldName == null || defaultFieldName.trim().isEmpty()) {
            throw new IllegalArgumentException("defaultField cannot be null, an empty string or only containing whitespace characters");
        }

        if (defaultFieldName.contains(FIELD_NAME_PREFIX) || defaultFieldName.contains(FIELD_NAME_SUFFIX)) {
            throw new IllegalArgumentException("defaultField cannot contain opening or closing braces");
        }

        PatternParseResult parseResult = parsePattern(pattern, defaultFieldName);
        this.resolvePattern = parseResult.resolvePattern;
        this.unresolvePattern = parseResult.unresolvePattern;
        this.fields = Collections.unmodifiableList(parseResult.fields);

        this.defaultFieldName = defaultFieldName;
        this.defaultFieldValues = Collections.unmodifiableMap(new HashMap<>(defaultFieldValues));
    }

    /**
     * Translates the internal representation of a name to the external one.
     *
     * @param defaultFieldValue the name to resolve
     * @return the resolved name
     */
    public String resolve(String defaultFieldValue) {
        var resolveFields = new HashMap<>(defaultFieldValues);
        resolveFields.put(defaultFieldName, defaultFieldValue);
        return new StringSubstitutor(resolveFields, FIELD_NAME_PREFIX, FIELD_NAME_SUFFIX)
                .setEnableUndefinedVariableException(true)
                .replace(resolvePattern);
    }

    /**
     * Translates the external representation of a name to the internal one.
     *
     * @param name the external name
     * @return the corresponding internal name
     */
    @Override
    public String unresolve(String name) {
        return unresolveContext(name).get(defaultFieldName);
    }

    /**
     * Decompose a string into a context map representing the different fields.
     *
     * @param name the value to convert
     * @return a map of the field names mapped to the value used by the input for the pattern field
     * @throws IllegalArgumentException if the input value does not match the pattern
     */
    public Map<String, String> unresolveContext(String name) {
        Matcher matcher = unresolvePattern.matcher(name);

        if (!matcher.matches() || matcher.groupCount() != fields.size()) {
            throw new IllegalArgumentException("Name '" + name + "' does not match pattern " + resolvePattern);
        }

        int groupIndex = 0;
        final Map<String, String> result = new HashMap<>();
        for (String fieldName : fields) {
            String matchedValue = matcher.group(++groupIndex);
            result.put(fieldName, matchedValue);
        }

        // Return read-only copy of the context map
        return Map.copyOf(result);
    }

    /**
     * Escape a string literal (series of characters) for use in a regex pattern
     *
     * @param literal the literal that needs escaping
     * @return the escaped literal for use in a regex
     */
    private static String escape(String literal) {
        var result = new StringBuilder();
        for (int index = 0; index < literal.length(); index++) {
            switch (literal.charAt(index)) {
                case '$', '#', '.', '{', '}' -> result.append("\\");
            }
            result.append(literal.charAt(index));
        }
        return result.toString();
    }

    private static String characterRegex(String characters) {
        return characterRegex(characters, false);
    }

    private static String characterRegex(String characters, boolean oneOrMore) {
        return "[" + escape(characters) + "]" + (oneOrMore ? "+" : "");
    }

    private static PatternParseResult parsePattern(final String pattern, final String defaultField) {
        var matcher = FIELD_NAME_OR_LITERAL_PATTERN.matcher(pattern);

        var fields = new ArrayList<String>();
        var pat = new StringBuilder();
        var count = 0;
        var pos = 0;
        var lastElementWasPlaceholder = false;
        pat.append("^");
        while (matcher.find()) {
            count++;
            if (matcher.start() != pos) {
                throw new IllegalArgumentException(String.format("Pattern contains faulty characters at position " + pos + ": %s", pattern));
            }
            var element = matcher.group();
            pos += element.length();
            if (element.startsWith(FIELD_NAME_PREFIX) && element.endsWith(FIELD_NAME_SUFFIX)) {
                // Treat the element as a placeholder
                if (count > 0 && lastElementWasPlaceholder) {
                    throw new IllegalArgumentException(String.format("Two consecutive placeholders found in pattern: %s", pattern));
                }
                var field = element.substring(1, element.length() - 1);
                fields.add(field);
                pat.append("(").append(field.equals(defaultField) ? DEFAULT_FIELD_VALUE_REGEX : FIELD_VALUE_REGEX).append(")");
                lastElementWasPlaceholder = true;
            } else {
                // Treat the element as a string literal
                if (count > 0 && !lastElementWasPlaceholder) {
                    throw new IllegalArgumentException(String.format("Two consecutive literals found in pattern: %s", pattern));
                }
                pat.append(escape(element));
                lastElementWasPlaceholder = false;
            }
        }
        pat.append("$");

        if (count == 0) {
            throw new IllegalArgumentException(String.format("Illegal pattern provided: %s", pattern));
        }

        return PatternParseResult.builder()
                .resolvePattern(pattern)
                .unresolvePattern(Pattern.compile(pat.toString()))
                .fields(fields)
                .build();
    }
}
