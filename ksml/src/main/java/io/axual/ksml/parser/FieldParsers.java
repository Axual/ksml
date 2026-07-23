package io.axual.ksml.parser;

/*-
 * ========================LICENSE_START=================================
 * KSML
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

import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.EnumSchema;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.schema.UnionSchema;
import io.axual.ksml.data.util.ListUtil;
import io.axual.ksml.exception.ParseException;
import io.axual.ksml.exception.TopologyException;
import io.axual.ksml.metric.MetricTags;
import io.axual.ksml.type.UserType;
import io.axual.ksml.util.SchemaUtil;
import lombok.Getter;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.axual.ksml.data.schema.DataSchemaConstants.NO_TAG;

// The stateless "parse a field and describe it as a schema fragment at the same time" DSL
// used to build parsers. Extracted from DefinitionParser so this toolkit is reachable without
// inheriting from it: nothing in this class depends on a parser's resources or identity.
public final class FieldParsers {
    public static final String SCHEMA_NAMESPACE = "io.axual.ksml";
    private static final Parser<String> CODE_STRING_PARSER = new StringValueParser(value -> value ? "True" : "False");
    private static final UserTypeParser USER_TYPE_PARSER = new UserTypeParser();
    private static final DataSchema CODE_SCHEMA = new UnionSchema(
            new UnionSchema.Member(DataSchema.BOOLEAN_SCHEMA),
            new UnionSchema.Member(DataSchema.LONG_SCHEMA),
            new UnionSchema.Member(DataSchema.DOUBLE_SCHEMA),
            new UnionSchema.Member(DataSchema.STRING_SCHEMA));
    private static final ParserWithSchema<String> CODE_PARSER = ParserWithSchema.of(CODE_STRING_PARSER::parse, CODE_SCHEMA);
    private static final DurationParser DURATION_PARSER = new DurationParser();

    private FieldParsers() {
    }

    public static <V> StructsParser<V> withDefault(StructsParser<V> parser, V defaultValue) {
        return new StructsParser<>() {
            @Override
            public List<StructSchema> schemas() {
                return parser.schemas();
            }

            @Override
            public V parse(ParseNode node) {
                final var result = parser.parse(node);
                return result != null ? result : defaultValue;
            }
        };
    }

    private static StructSchema.Field optional(StructSchema.Field field) {
        return field.required()
                ? new StructSchema.Field(field.name(), field.schema(), "*(optional)* " + field.doc(), field.tag(), false, field.constant(), field.defaultValue())
                : field;
    }

    private static List<StructSchema.Field> optional(List<StructSchema.Field> fields) {
        return fields.stream().map(FieldParsers::optional).toList();
    }

    private static StructSchema optional(StructSchema schema) {
        if (schema.fields().isEmpty()) return schema;
        return new StructSchema(schema.namespace(), schema.name(), schema.doc(), optional(schema.fields()), false);
    }

    public static <V> StructsParser<V> optional(StructsParser<V> parser, V valueIfMissing) {
        final var newSchemas = parser.schemas().stream().map(FieldParsers::optional).toList();
        return StructsParser.of(node -> node != null ? parser.parse(node) : valueIfMissing, newSchemas);
    }

    public static <V> StructsParser<V> optional(StructsParser<V> parser) {
        return optional(parser, null);
    }

    public static StructSchema structSchema(Class<?> clazz, String doc, List<StructSchema.Field> fields) {
        return structSchema(clazz.getSimpleName(), doc, fields);
    }

    public static StructSchema structSchema(String name, String doc, List<StructSchema.Field> fields) {
        return new StructSchema(SCHEMA_NAMESPACE, name, doc, fields, false);
    }

    private static class FieldParser<V> implements StructsParser<V> {
        private final StructSchema.Field field;
        @Getter
        private final List<StructSchema> schemas;
        private final ParserWithSchemas<V> valueParser;

        public FieldParser(String childName, boolean constant, String doc, ParserWithSchema<V> valueParser) {
            this(childName, constant, doc, ParserWithSchemas.of(valueParser::parse, valueParser.schema()));
        }

        public FieldParser(String childName, boolean constant, String doc, ParserWithSchemas<V> valueParser) {
            final var parsedSchemas = valueParser.schemas();
            final var fieldSchema = parsedSchemas.size() == 1
                    ? parsedSchemas.getFirst()
                    : new UnionSchema(parsedSchemas.stream().map(UnionSchema.Member::new).toArray(UnionSchema.Member[]::new));
            field = new StructSchema.Field(childName, fieldSchema, doc, NO_TAG, true, constant, null);
            schemas = List.of(structSchema((String) null, null, List.of(field)));
            this.valueParser = valueParser;
        }

        @Override
        public V parse(ParseNode node) {
            try {
                if (node == null) return null;
                final var child = node.get(field.name());
                return child != null ? valueParser.parse(child) : null;
            } catch (Exception e) {
                throw new ParseException(node, e.getMessage(), e);
            }
        }
    }

    private static class ValueStructParser<V> implements StructsParser<V> {
        @Getter
        private final List<StructSchema> schemas = new ArrayList<>();
        private final Parser<V> valueParser;

        public ValueStructParser(String name, String doc, List<StructsParser<?>> parsers, Parser<V> valueParser) {
            for (final var parser : parsers) {
                SchemaUtil.addToSchemas(schemas, name, doc, parser);
            }
            this.valueParser = valueParser;
        }

        @Override
        public V parse(ParseNode node) {
            try {
                return valueParser.parse(node);
            } catch (Exception e) {
                throw new ParseException(node, e.getMessage(), e);
            }
        }
    }

    public static <V> StructsParser<V> freeField(String childName, String doc, ParserWithSchemas<V> parser) {
        return new FieldParser<>(childName, false, doc, parser);
    }

    public static StructsParser<Boolean> booleanField(String childName, String doc) {
        return freeField(childName, doc, ParserWithSchemas.of(ParseNode::asBoolean, DataSchema.BOOLEAN_SCHEMA));
    }

    public static StructsParser<Duration> durationField(String childName, String doc) {
        return freeField(childName, doc, DURATION_PARSER);
    }

    public static StructsParser<String> enumField(String childName, EnumSchema schema) {
        final var stringParser = new StringValueParser();
        final ParserWithSchema<String> enumParser = ParserWithSchema.of(
                node -> {
                    final var value = stringParser.parse(node);
                    if (value == null)
                        throw new ParseException(node, "Empty value not allowed for enum " + schema.name());
                    if (ListUtil.find(schema.symbols(), symbol -> symbol.name().equals(value)) == null)
                        throw new ParseException(node, "Illegal value for enum " + schema.name() + ": " + value);
                    return value;
                },
                schema);
        return new FieldParser<>(childName, false, schema.doc(), enumParser);
    }

    public static StructsParser<Integer> integerField(String childName, String doc) {
        return freeField(childName, doc, ParserWithSchemas.of(node -> {
            if (!node.isInt())
                throw new ParseException(node, "YAML value for '" + childName + "' is out of INT range: " + node.asString());
            return node.asInt();
        }, DataSchema.INTEGER_SCHEMA));
    }

    public static StructsParser<Long> longField(String childName, String doc) {
        return freeField(childName, doc, ParserWithSchemas.of(node -> {
            if (!node.isInt() && !node.isLong())
                throw new ParseException(node, "YAML value for '" + childName + "' is not a valid long integer: " + node.asString());
            return node.asLong();
        }, DataSchema.LONG_SCHEMA));
    }

    public static StructsParser<String> stringField(String childName, String doc) {
        return stringField(childName, false, doc);
    }

    public static StructsParser<String> stringField(String childName, boolean constant, String doc) {
        return new FieldParser<>(childName, constant, doc, new StringValueParser());
    }

    public static StructsParser<String> codeField(String childName, String doc) {
        return new FieldParser<>(childName, false, doc, CODE_PARSER);
    }

    public static StructsParser<UserType> userTypeField(String childName, String doc, boolean allowUnresolved) {
        return userTypeField(childName, doc, allowUnresolved, null);
    }

    public static StructsParser<UserType> userTypeField(String childName, String doc, boolean allowUnresolved, UserType defaultValue) {
        final var stringParser = stringField(childName, doc);
        final var field = new StructSchema.Field(childName, DataSchema.STRING_SCHEMA, doc, NO_TAG, true, false, null);
        final var schemas = structSchema((String) null, null, List.of(field));
        return StructsParser.of(node -> {
                    final var content = stringParser.parse(node);
                    if (content == null) return defaultValue;
                    final var parsedContent = USER_TYPE_PARSER.parse(content, allowUnresolved);
                    if (parsedContent.isError()) throw new ParseException(node, parsedContent.errorMessage());
                    return parsedContent.result();
                },
                schemas);
    }

    public static <U> StructsParser<List<U>> listField(String childName, String childTagKey, String childTagValuePrefix, String doc, ParserWithSchemas<U> valueParser) {
        return new FieldParser<>(childName, false, doc, new ListWithSchemaParser<>(childTagKey, childTagValuePrefix, valueParser)) {
            @Override
            public List<U> parse(ParseNode node) {
                final var list = super.parse(node);
                return list != null ? list : new ArrayList<>();
            }
        };
    }

    public static <U> StructsParser<Map<String, U>> mapField(String childName, String childTagKey, String childTagValuePrefix, String doc, ParserWithSchemas<U> valueParser) {
        return new FieldParser<>(childName, false, doc, new MapWithSchemaParser<>(childTagKey, childTagValuePrefix, valueParser)) {
            @Override
            public Map<String, U> parse(ParseNode node) {
                final var map = super.parse(node);
                return map != null ? map : new HashMap<>();
            }
        };
    }

    public static <U> StructsParser<U> customField(String childName, String doc, ParserWithSchemas<U> valueParser) {
        return new FieldParser<>(childName, false, doc, valueParser);
    }

    public static String validateName(String objectType, String parsedName, String defaultName) {
        return validateName(objectType, parsedName, defaultName, true);
    }

    public static String validateName(String objectType, String parsedName, String defaultName, boolean required) {
        if (parsedName != null) return parsedName;
        if (defaultName != null) return defaultName;
        if (!required) return null;
        return parseError(objectType + " name not defined");
    }

    public static <V> V parseError(String message) {
        throw new TopologyException(message);
    }

    // Constructor0..Constructor10 and the structsParser(...) overloads below are an
    // arity-indexed family: one hand-written interface/method per arity, because Java has
    // no variadic generics to express "N typed StructsParser arguments, zipped into one
    // constructor call" any other way. Parameter count necessarily grows with arity, so
    // java:S107 ("too many parameters") is expected and accepted from Constructor7/
    // structsParser(...,G,...) upward; suppressed individually below rather than disabling
    // the rule project-wide.
    public interface Constructor0<R> {
        R construct(MetricTags tags);
    }

    public interface Constructor1<R, A> {
        R construct(A a, MetricTags tags);
    }

    public interface Constructor2<R, A, B> {
        R construct(A a, B b, MetricTags tags);
    }

    public interface Constructor3<R, A, B, C> {
        R construct(A a, B b, C c, MetricTags tags);
    }

    public interface Constructor4<R, A, B, C, D> {
        R construct(A a, B b, C c, D d, MetricTags tags);
    }

    public interface Constructor5<R, A, B, C, D, E> {
        R construct(A a, B b, C c, D d, E e, MetricTags tags);
    }

    public interface Constructor6<R, A, B, C, D, E, F> {
        R construct(A a, B b, C c, D d, E e, F f, MetricTags tags);
    }

    public interface Constructor7<R, A, B, C, D, E, F, G> {
        @SuppressWarnings("java:S107")
        R construct(A a, B b, C c, D d, E e, F f, G g, MetricTags tags);
    }

    public interface Constructor8<R, A, B, C, D, E, F, G, H> {
        @SuppressWarnings("java:S107")
        R construct(A a, B b, C c, D d, E e, F f, G g, H h, MetricTags tags);
    }

    public interface Constructor9<R, A, B, C, D, E, F, G, H, I> {
        @SuppressWarnings("java:S107")
        R construct(A a, B b, C c, D d, E e, F f, G g, H h, I i, MetricTags tags);
    }

    public interface Constructor10<R, A, B, C, D, E, F, G, H, I, J> {
        @SuppressWarnings("java:S107")
        R construct(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, MetricTags tags);
    }

    public static <S> StructsParser<S> structsParser(Class<S> resultClass, String definitionVariant, String doc, Constructor0<S> constructor) {
        return new ValueStructParser<>(resultClass.getSimpleName() + definitionVariant, doc, List.of(), node -> constructor.construct(node.tags()));
    }

    public static <S, A> StructsParser<S> structsParser(Class<S> resultClass, String definitionVariant, String doc, StructsParser<A> a, Constructor1<S, A> constructor) {
        return new ValueStructParser<>(resultClass.getSimpleName() + definitionVariant, doc, List.of(a), node -> constructor.construct(a.parse(node), node.tags()));
    }

    public static <S, A, B> StructsParser<S> structsParser(Class<? extends S> resultClass, String definitionVariant, String doc, StructsParser<A> a, StructsParser<B> b, Constructor2<S, A, B> constructor) {
        return new ValueStructParser<>(resultClass.getSimpleName() + definitionVariant, doc, List.of(a, b), node -> constructor.construct(a.parse(node), b.parse(node), node.tags()));
    }

    public static <S, A, B, C> StructsParser<S> structsParser(Class<? extends S> resultClass, String definitionVariant, String doc, StructsParser<A> a, StructsParser<B> b, StructsParser<C> c, Constructor3<S, A, B, C> constructor) {
        return new ValueStructParser<>(resultClass.getSimpleName() + definitionVariant, doc, List.of(a, b, c), node -> constructor.construct(a.parse(node), b.parse(node), c.parse(node), node.tags()));
    }

    @SuppressWarnings("java:S107")
    public static <S, A, B, C, D> StructsParser<S> structsParser(Class<? extends S> resultClass, String definitionVariant, String doc, StructsParser<A> a, StructsParser<B> b, StructsParser<C> c, StructsParser<D> d, Constructor4<S, A, B, C, D> constructor) {
        return new ValueStructParser<>(resultClass.getSimpleName() + definitionVariant, doc, List.of(a, b, c, d), node -> constructor.construct(a.parse(node), b.parse(node), c.parse(node), d.parse(node), node.tags()));
    }

    @SuppressWarnings("java:S107")
    public static <S, A, B, C, D, E> StructsParser<S> structsParser(Class<? extends S> resultClass, String definitionVariant, String doc, StructsParser<A> a, StructsParser<B> b, StructsParser<C> c, StructsParser<D> d, StructsParser<E> e, Constructor5<S, A, B, C, D, E> constructor) {
        return new ValueStructParser<>(resultClass.getSimpleName() + definitionVariant, doc, List.of(a, b, c, d, e), node -> constructor.construct(a.parse(node), b.parse(node), c.parse(node), d.parse(node), e.parse(node), node.tags()));
    }

    @SuppressWarnings("java:S107")
    public static <S, A, B, C, D, E, F> StructsParser<S> structsParser(Class<? extends S> resultClass, String definitionVariant, String doc, StructsParser<A> a, StructsParser<B> b, StructsParser<C> c, StructsParser<D> d, StructsParser<E> e, StructsParser<F> f, Constructor6<S, A, B, C, D, E, F> constructor) {
        return new ValueStructParser<>(resultClass.getSimpleName() + definitionVariant, doc, List.of(a, b, c, d, e, f), node -> constructor.construct(a.parse(node), b.parse(node), c.parse(node), d.parse(node), e.parse(node), f.parse(node), node.tags()));
    }

    @SuppressWarnings("java:S107")
    public static <S, A, B, C, D, E, F, G> StructsParser<S> structsParser(Class<? extends S> resultClass, String definitionVariant, String doc, StructsParser<A> a, StructsParser<B> b, StructsParser<C> c, StructsParser<D> d, StructsParser<E> e, StructsParser<F> f, StructsParser<G> g, Constructor7<S, A, B, C, D, E, F, G> constructor) {
        return new ValueStructParser<>(resultClass.getSimpleName() + definitionVariant, doc, List.of(a, b, c, d, e, f, g), node -> constructor.construct(a.parse(node), b.parse(node), c.parse(node), d.parse(node), e.parse(node), f.parse(node), g.parse(node), node.tags()));
    }

    @SuppressWarnings("java:S107")
    public static <S, A, B, C, D, E, F, G, H> StructsParser<S> structsParser(Class<? extends S> resultClass, String definitionVariant, String doc, StructsParser<A> a, StructsParser<B> b, StructsParser<C> c, StructsParser<D> d, StructsParser<E> e, StructsParser<F> f, StructsParser<G> g, StructsParser<H> h, Constructor8<S, A, B, C, D, E, F, G, H> constructor) {
        return new ValueStructParser<>(resultClass.getSimpleName() + definitionVariant, doc, List.of(a, b, c, d, e, f, g, h), node -> constructor.construct(a.parse(node), b.parse(node), c.parse(node), d.parse(node), e.parse(node), f.parse(node), g.parse(node), h.parse(node), node.tags()));
    }

    @SuppressWarnings("java:S107")
    public static <S, A, B, C, D, E, F, G, H, I> StructsParser<S> structsParser(Class<? extends S> resultClass, String definitionVariant, String doc, StructsParser<A> a, StructsParser<B> b, StructsParser<C> c, StructsParser<D> d, StructsParser<E> e, StructsParser<F> f, StructsParser<G> g, StructsParser<H> h, StructsParser<I> i, Constructor9<S, A, B, C, D, E, F, G, H, I> constructor) {
        return new ValueStructParser<>(resultClass.getSimpleName() + definitionVariant, doc, List.of(a, b, c, d, e, f, g, h, i), node -> constructor.construct(a.parse(node), b.parse(node), c.parse(node), d.parse(node), e.parse(node), f.parse(node), g.parse(node), h.parse(node), i.parse(node), node.tags()));
    }

    @SuppressWarnings("java:S107")
    public static <S, A, B, C, D, E, F, G, H, I, J> StructsParser<S> structsParser(Class<? extends S> resultClass, String definitionVariant, String doc, StructsParser<A> a, StructsParser<B> b, StructsParser<C> c, StructsParser<D> d, StructsParser<E> e, StructsParser<F> f, StructsParser<G> g, StructsParser<H> h, StructsParser<I> i, StructsParser<J> j, Constructor10<S, A, B, C, D, E, F, G, H, I, J> constructor) {
        return new ValueStructParser<>(resultClass.getSimpleName() + definitionVariant, doc, List.of(a, b, c, d, e, f, g, h, i, j), node -> constructor.construct(a.parse(node), b.parse(node), c.parse(node), d.parse(node), e.parse(node), f.parse(node), g.parse(node), h.parse(node), i.parse(node), j.parse(node), node.tags()));
    }
}
