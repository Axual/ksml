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

import io.axual.ksml.client.exception.InvalidPatternException;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.text.StringSubstitutor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class PatternResolver implements Resolver {
    protected static final String FIELD_NAME_PREFIX = "{";
    protected static final String FIELD_NAME_SUFFIX = "}";
    // Matches a literal separator run of $, # or - characters, e.g. "--" or "$#"
    private static final String LITERAL_REGEX = "[$#-]+";
    // Matches a field value: alphanumeric, underscore or dot
    private static final String FIELD_VALUE_REGEX = "[a-zA-Z0-9_.]+";
    // Like FIELD_VALUE_REGEX, but also allows '-' since the default field (e.g. a topic name) may contain dashes
    private static final String DEFAULT_FIELD_VALUE_REGEX = "[a-zA-Z0-9_.-]+";
    // Matches a {fieldName} placeholder, e.g. "{tenant}"
    private static final String FIELD_NAME_REGEX = "\\{[a-zA-Z0-9_.]+\\}";
    private static final String FIELD_NAME_OR_LITERAL_MATCH_REGEX = "(" + FIELD_NAME_REGEX + "|" + LITERAL_REGEX + ")";
    private static final Pattern FIELD_NAME_OR_LITERAL_PATTERN = Pattern.compile(FIELD_NAME_OR_LITERAL_MATCH_REGEX);
    private final Map<String, String> defaultFieldValues;
    @Getter(value = AccessLevel.PACKAGE)
    private final String resourceFieldName;
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
     * @param resourceFieldName the resource type that the pattern should end with
     * @throws InvalidPatternException thrown when the pattern is null or malformed
     * @throws IllegalArgumentException thrown the resourceFieldName is null or malformed
     */
    public PatternResolver(final String pattern, final String resourceFieldName, Map<String, String> defaultFieldValues) {
        if (pattern == null || pattern.trim().isEmpty()) {
            throw new InvalidPatternException(pattern, "pattern cannot be null or empty");
        }

        if (resourceFieldName == null || resourceFieldName.trim().isEmpty()) {
            throw new IllegalArgumentException("resourceFieldName cannot be null, an empty string or only containing whitespace characters");
        }

        if (resourceFieldName.contains(FIELD_NAME_PREFIX) || resourceFieldName.contains(FIELD_NAME_SUFFIX)) {
            throw new IllegalArgumentException("resourceFieldName cannot contain opening or closing braces");
        }

        checkBraceBalanced(pattern);

        final PatternParseResult parseResult = parsePattern(pattern, resourceFieldName);

        this.resolvePattern = parseResult.resolvePattern;
        this.unresolvePattern = parseResult.unresolvePattern;
        this.fields = Collections.unmodifiableList(parseResult.fields);

        // Validate that the resourceFieldName is used in the pattern
        if (!this.fields.contains(resourceFieldName)) {
            throw new InvalidPatternException(pattern, "The resourceFieldName %s is not used in the pattern".formatted(resourceFieldName));
        }
        final var validFieldNames = new HashSet<>(defaultFieldValues.keySet());
        // Add resourceFieldName to the list of valid names
        validFieldNames.add(resourceFieldName);

        // Check for unknown field names in the pattern,
        var unknownFieldNames = new ArrayList<>(parseResult.fields);
        unknownFieldNames.removeIf(validFieldNames::contains);
        if (!unknownFieldNames.isEmpty()) {
            throw new InvalidPatternException(pattern, "Unknown field names used in the pattern: %s".formatted(unknownFieldNames));
        }

        this.resourceFieldName = resourceFieldName;
        this.defaultFieldValues = Collections.unmodifiableMap(new HashMap<>(defaultFieldValues));
    }

    /**
     * Translates the internal representation of a name to the external one.
     *
     * @param resourceFieldValue the name to resolve
     * @return the resolved name
     */
    public String resolve(String resourceFieldValue) {
        var resolveFields = new HashMap<>(defaultFieldValues);
        resolveFields.put(resourceFieldName, resourceFieldValue);
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
        return unresolveContext(name).get(resourceFieldName);
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
                default -> { /* no escaping needed for other characters */ }
            }
            result.append(literal.charAt(index));
        }
        return result.toString();
    }

    /**
     * Parses a pattern string into the pieces needed to resolve and unresolve names against it.
     *
     * @param pattern           the pattern to parse, e.g. {@code "{tenant}--{instance}--{topic}"}.
     * @param resourceFieldName the placeholder in {@code pattern} that identifies the resource field.
     * @return the parsed pattern: the original pattern string (used by {@link #resolve}), a compiled
     * regex that extracts field values from a resolved name (used by {@link #unresolveContext}),
     * and the ordered list of field names the pattern declares.
     * @throws InvalidPatternException if the pattern is not a valid, gapless sequence of alternating
     *                                 placeholders and literals.
     */
    private static PatternParseResult parsePattern(final String pattern, final String resourceFieldName) {
        // Check that placeholders and literals strictly alternate, with no gaps between them
        checkTokensAlternate(pattern);

        var matcher = FIELD_NAME_OR_LITERAL_PATTERN.matcher(pattern);

        var fields = new ArrayList<String>();
        var pat = new StringBuilder("^");
        while (matcher.find()) {
            var element = matcher.group();
            if (element.startsWith(FIELD_NAME_PREFIX) && element.endsWith(FIELD_NAME_SUFFIX)) {
                // Treat the element as a placeholder
                var field = element.substring(1, element.length() - 1);
                fields.add(field);
                pat.append("(").append(field.equals(resourceFieldName) ? DEFAULT_FIELD_VALUE_REGEX : FIELD_VALUE_REGEX).append(")");
            } else {
                // Treat the element as a string literal
                pat.append(escape(element));
            }
        }
        pat.append("$");

        return PatternParseResult.builder()
                .resolvePattern(pattern)
                .unresolvePattern(Pattern.compile(pat.toString()))
                .fields(fields)
                .build();
    }

    /**
     * Validate that the pattern tokenizes into a gapless, strictly alternating sequence of
     * placeholders and literals (e.g. placeholder-literal-placeholder, never placeholder-placeholder),
     * and that at least one token is found.
     */
    private static void checkTokensAlternate(String pattern) {
        var matcher = FIELD_NAME_OR_LITERAL_PATTERN.matcher(pattern);
        var count = 0;
        var pos = 0;
        var lastElementWasPlaceholder = false;
        while (matcher.find()) {
            count++;
            if (matcher.start() != pos) {
                throw new InvalidPatternException(pattern, "Faulty characters detected at position %d".formatted(pos));
            }
            var element = matcher.group();
            pos += element.length();
            var isPlaceholder = element.startsWith(FIELD_NAME_PREFIX) && element.endsWith(FIELD_NAME_SUFFIX);
            if (isPlaceholder == lastElementWasPlaceholder) {
                throw new InvalidPatternException(pattern, isPlaceholder ? "Two consecutive placeholders found" : "Two consecutive literals found");
            }
            lastElementWasPlaceholder = isPlaceholder;
        }

        if (count == 0) {
            throw new InvalidPatternException(pattern, "No fields found");
        }
    }

    /**
     * Check that the pattern has balanced braces, i.e. that each opening brace has a corresponding closing brace.
     * @param pattern the pattern to check.
     * @throws InvalidPatternException if unbalanced braces were found in the pattern.
     */
    private static void checkBraceBalanced(String pattern) {
        var openPosition = Integer.MIN_VALUE;
        for (int position = 0; position < pattern.length(); position++) {
            final var character = pattern.charAt(position);
            if (character == '{') {
                if (openPosition >= 0) {
                    throw new InvalidPatternException(pattern, "Found open brace at position %d without closing previous open brace at position %d".formatted(position, openPosition));
                }
                openPosition = position;
            } else if (character == '}') {
                if (openPosition < 0) {
                    throw new InvalidPatternException(pattern, "Found close brace at position %d with no corresponding open brace".formatted(position));
                }
                // Found corresponding brace
                openPosition = Integer.MIN_VALUE;
            }
        }

        if (openPosition >= 0) {
            throw new InvalidPatternException(pattern, "Found open brace at position %d with no corresponding close brace".formatted(openPosition));
        }
    }
}
