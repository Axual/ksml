package io.axual.ksml.parser;

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

import io.axual.ksml.data.schema.DataField;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.EnumSchema;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.schema.UnionSchema;
import io.axual.ksml.data.util.ListUtil;
import io.axual.ksml.exception.ParseException;
import io.axual.ksml.exception.TopologyException;
import io.axual.ksml.execution.FatalError;
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

public abstract class DefinitionParser<T> extends BaseParser<T> implements StructsParser<T> {
    public static final String SCHEMA_NAMESPACE = "io.axual.ksml";
    private static final Parser<String> CODE_STRING_PARSER = new StringValueParser(value -> value ? "True" : "False");
    private static final DataSchema CODE_SCHEMA = new UnionSchema(
            new UnionSchema.Member(DataSchema.BOOLEAN_SCHEMA),
            new UnionSchema.Member(DataSchema.LONG_SCHEMA),
            new UnionSchema.Member(DataSchema.DOUBLE_SCHEMA),
            new UnionSchema.Member(DataSchema.STRING_SCHEMA));
    protected static final ParserWithSchema<String> CODE_PARSER = ParserWithSchema.of(CODE_STRING_PARSER::parse, CODE_SCHEMA);
    private final DurationParser durationParser = new DurationParser();
    private StructsParser<T> parser;

    protected abstract StructsParser<T> parser();

    @Override
    public final T parse(ParseNode node) {
        if (parser == null) parser = parser();
        try {
            return parser.parse(node);
        } catch (Exception e) {
            throw FatalError.report(e);
        }
    }

    @Override
    public final List<StructSchema> schemas() {
        if (parser == null) parser = parser();
        return parser.schemas();
    }

    protected static <V> StructsParser<V> optional(StructsParser<V> parser) {
        return optional(parser, null);
    }

    protected static <V> StructsParser<V> withDefault(StructsParser<V> parser, V defaultValue) {
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

    protected static <V> StructsParser<V> optional(StructsParser<V> parser, V valueIfMissing) {
        final var newSchemas = new ArrayList<StructSchema>();
        for (final var schema : parser.schemas()) {
            if (!schema.fields().isEmpty()) {
                final var newFields = schema.fields().stream()
                        .map(field -> field.required() ? new DataField(field.name(), field.schema(), "*(optional)* " + field.doc(), field.tag(), false, field.constant(), field.defaultValue()) : field)
                        .toList();
                newSchemas.add(new StructSchema(schema.namespace(), schema.name(), schema.doc(), newFields, false));
            } else {
                newSchemas.add(schema);
            }
        }
        return StructsParser.of(node -> node != null ? parser.parse(node) : valueIfMissing, newSchemas);
    }

    protected static StructSchema structSchema(Class<?> clazz, String doc, List<DataField> fields) {
        return structSchema(clazz.getSimpleName(), doc, fields);
    }

    protected static StructSchema structSchema(String name, String doc, List<DataField> fields) {
        return new StructSchema(SCHEMA_NAMESPACE, name, doc, fields, false);
    }

    private static class FieldParser<V> implements StructsParser<V> {
        private final DataField field;
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
            field = new DataField(childName, fieldSchema, doc, NO_TAG, true, constant, null);
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

    protected <V> StructsParser<V> freeField(String childName, String doc, ParserWithSchemas<V> parser) {
        return new FieldParser<>(childName, false, doc, parser);
    }

    protected StructsParser<Boolean> booleanField(String childName, String doc) {
        return freeField(childName, doc, ParserWithSchemas.of(ParseNode::asBoolean, DataSchema.BOOLEAN_SCHEMA));
    }

    protected StructsParser<Duration> durationField(String childName, String doc) {
        return freeField(childName, doc, durationParser);
    }

    protected StructsParser<String> enumField(String childName, EnumSchema schema) {
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

    protected StructsParser<Integer> integerField(String childName, String doc) {
        return freeField(childName, doc, ParserWithSchemas.of(ParseNode::asInt, DataSchema.INTEGER_SCHEMA));
    }

    protected StructsParser<Long> longField(String childName, String doc) {
        return freeField(childName, doc, ParserWithSchemas.of(ParseNode::asLong, DataSchema.LONG_SCHEMA));
    }

    protected StructsParser<String> stringField(String childName, String doc) {
        return stringField(childName, false, doc);
    }

    protected StructsParser<String> stringField(String childName, boolean constant, String doc) {
        return new FieldParser<>(childName, constant, doc, new StringValueParser());
    }

    protected StructsParser<String> codeField(String childName, String doc) {
        return new FieldParser<>(childName, false, doc, CODE_PARSER);
    }

    protected StructsParser<UserType> userTypeField(String childName, String doc) {
        return userTypeField(childName, doc, null);
    }

    protected StructsParser<UserType> userTypeField(String childName, String doc, UserType defaultValue) {
        final var stringParser = stringField(childName, doc);
        final var field = new DataField(childName, DataSchema.STRING_SCHEMA, doc, NO_TAG, true, false, null);
        final var schemas = structSchema((String) null, null, List.of(field));
        return StructsParser.of(node -> {
                    final var content = stringParser.parse(node);
                    if (content == null) return defaultValue;
                    final var parsedContent = UserTypeParser.parse(content);
                    if (parsedContent.isError()) throw new ParseException(node, parsedContent.errorMessage());
                    return parsedContent.result();
                },
                schemas);
    }

    protected <TYPE> StructsParser<List<TYPE>> listField(String childName, String childTagKey, String childTagValuePrefix, String doc, ParserWithSchemas<TYPE> valueParser) {
        return new FieldParser<>(childName, false, doc, new ListWithSchemaParser<>(childTagKey, childTagValuePrefix, valueParser)) {
            @Override
            public List<TYPE> parse(ParseNode node) {
                final var list = super.parse(node);
                return list != null ? list : new ArrayList<>();
            }
        };
    }

    protected <TYPE> StructsParser<Map<String, TYPE>> mapField(String childName, String childTagKey, String childTagValuePrefix, String doc, ParserWithSchemas<TYPE> valueParser) {
        return new FieldParser<>(childName, false, doc, new MapWithSchemaParser<>(childTagKey, childTagValuePrefix, valueParser)) {
            @Override
            public Map<String, TYPE> parse(ParseNode node) {
                final var map = super.parse(node);
                return map != null ? map : new HashMap<>();
            }
        };
    }

    protected <TYPE> StructsParser<TYPE> customField(String childName, String doc, ParserWithSchemas<TYPE> valueParser) {
        return new FieldParser<>(childName, false, doc, valueParser);
    }

    protected String validateName(String objectType, String parsedName, String defaultName) {
        return validateName(objectType, parsedName, defaultName, true);
    }

    protected String validateName(String objectType, String parsedName, String defaultName, boolean required) {
        if (parsedName != null) return parsedName;
        if (defaultName != null) return defaultName;
        if (!required) return null;
        return parseError(objectType + " name not defined");
    }

    protected <V> V parseError(String message) {
        throw new TopologyException(message);
    }

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
        R construct(A a, B b, C c, D d, E e, F f, G g, MetricTags tags);
    }

    public interface Constructor8<R, A, B, C, D, E, F, G, H> {
        R construct(A a, B b, C c, D d, E e, F f, G g, H h, MetricTags tags);
    }

    public interface Constructor9<R, A, B, C, D, E, F, G, H, I> {
        R construct(A a, B b, C c, D d, E e, F f, G g, H h, I i, MetricTags tags);
    }

    public interface Constructor10<R, A, B, C, D, E, F, G, H, I, J> {
        R construct(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, MetricTags tags);
    }

    protected <S> StructsParser<S> structsParser(Class<S> resultClass, String definitionVariant, String doc, Constructor0<S> constructor) {
        return new ValueStructParser<>(resultClass.getSimpleName() + definitionVariant, doc, List.of(), node -> constructor.construct(node.tags()));
    }

    protected <S, A> StructsParser<S> structsParser(Class<S> resultClass, String definitionVariant, String doc, StructsParser<A> a, Constructor1<S, A> constructor) {
        return new ValueStructParser<>(resultClass.getSimpleName() + definitionVariant, doc, List.of(a), node -> constructor.construct(a.parse(node), node.tags()));
    }

    protected <S, A, B> StructsParser<S> structsParser(Class<? extends S> resultClass, String definitionVariant, String doc, StructsParser<A> a, StructsParser<B> b, Constructor2<S, A, B> constructor) {
        return new ValueStructParser<>(resultClass.getSimpleName() + definitionVariant, doc, List.of(a, b), node -> constructor.construct(a.parse(node), b.parse(node), node.tags()));
    }

    protected <S, A, B, C> StructsParser<S> structsParser(Class<? extends S> resultClass, String definitionVariant, String doc, StructsParser<A> a, StructsParser<B> b, StructsParser<C> c, Constructor3<S, A, B, C> constructor) {
        return new ValueStructParser<>(resultClass.getSimpleName() + definitionVariant, doc, List.of(a, b, c), node -> constructor.construct(a.parse(node), b.parse(node), c.parse(node), node.tags()));
    }

    protected <S, A, B, C, D> StructsParser<S> structsParser(Class<? extends S> resultClass, String definitionVariant, String doc, StructsParser<A> a, StructsParser<B> b, StructsParser<C> c, StructsParser<D> d, Constructor4<S, A, B, C, D> constructor) {
        return new ValueStructParser<>(resultClass.getSimpleName() + definitionVariant, doc, List.of(a, b, c, d), node -> constructor.construct(a.parse(node), b.parse(node), c.parse(node), d.parse(node), node.tags()));
    }

    protected <S, A, B, C, D, E> StructsParser<S> structsParser(Class<? extends S> resultClass, String definitionVariant, String doc, StructsParser<A> a, StructsParser<B> b, StructsParser<C> c, StructsParser<D> d, StructsParser<E> e, Constructor5<S, A, B, C, D, E> constructor) {
        return new ValueStructParser<>(resultClass.getSimpleName() + definitionVariant, doc, List.of(a, b, c, d, e), node -> constructor.construct(a.parse(node), b.parse(node), c.parse(node), d.parse(node), e.parse(node), node.tags()));
    }

    protected <S, A, B, C, D, E, F> StructsParser<S> structsParser(Class<? extends S> resultClass, String definitionVariant, String doc, StructsParser<A> a, StructsParser<B> b, StructsParser<C> c, StructsParser<D> d, StructsParser<E> e, StructsParser<F> f, Constructor6<S, A, B, C, D, E, F> constructor) {
        return new ValueStructParser<>(resultClass.getSimpleName() + definitionVariant, doc, List.of(a, b, c, d, e, f), node -> constructor.construct(a.parse(node), b.parse(node), c.parse(node), d.parse(node), e.parse(node), f.parse(node), node.tags()));
    }

    protected <S, A, B, C, D, E, F, G> StructsParser<S> structsParser(Class<? extends S> resultClass, String definitionVariant, String doc, StructsParser<A> a, StructsParser<B> b, StructsParser<C> c, StructsParser<D> d, StructsParser<E> e, StructsParser<F> f, StructsParser<G> g, Constructor7<S, A, B, C, D, E, F, G> constructor) {
        return new ValueStructParser<>(resultClass.getSimpleName() + definitionVariant, doc, List.of(a, b, c, d, e, f, g), node -> constructor.construct(a.parse(node), b.parse(node), c.parse(node), d.parse(node), e.parse(node), f.parse(node), g.parse(node), node.tags()));
    }

    protected <S, A, B, C, D, E, F, G, H> StructsParser<S> structsParser(Class<? extends S> resultClass, String definitionVariant, String doc, StructsParser<A> a, StructsParser<B> b, StructsParser<C> c, StructsParser<D> d, StructsParser<E> e, StructsParser<F> f, StructsParser<G> g, StructsParser<H> h, Constructor8<S, A, B, C, D, E, F, G, H> constructor) {
        return new ValueStructParser<>(resultClass.getSimpleName() + definitionVariant, doc, List.of(a, b, c, d, e, f, g, h), node -> constructor.construct(a.parse(node), b.parse(node), c.parse(node), d.parse(node), e.parse(node), f.parse(node), g.parse(node), h.parse(node), node.tags()));
    }

    protected <S, A, B, C, D, E, F, G, H, I> StructsParser<S> structsParser(Class<? extends S> resultClass, String definitionVariant, String doc, StructsParser<A> a, StructsParser<B> b, StructsParser<C> c, StructsParser<D> d, StructsParser<E> e, StructsParser<F> f, StructsParser<G> g, StructsParser<H> h, StructsParser<I> i, Constructor9<S, A, B, C, D, E, F, G, H, I> constructor) {
        return new ValueStructParser<>(resultClass.getSimpleName() + definitionVariant, doc, List.of(a, b, c, d, e, f, g, h, i), node -> constructor.construct(a.parse(node), b.parse(node), c.parse(node), d.parse(node), e.parse(node), f.parse(node), g.parse(node), h.parse(node), i.parse(node), node.tags()));
    }

    protected <S, A, B, C, D, E, F, G, H, I, J> StructsParser<S> structsParser(Class<? extends S> resultClass, String definitionVariant, String doc, StructsParser<A> a, StructsParser<B> b, StructsParser<C> c, StructsParser<D> d, StructsParser<E> e, StructsParser<F> f, StructsParser<G> g, StructsParser<H> h, StructsParser<I> i, StructsParser<J> j, Constructor10<S, A, B, C, D, E, F, G, H, I, J> constructor) {
        return new ValueStructParser<>(resultClass.getSimpleName() + definitionVariant, doc, List.of(a, b, c, d, e, f, g, h, i, j), node -> constructor.construct(a.parse(node), b.parse(node), c.parse(node), d.parse(node), e.parse(node), f.parse(node), g.parse(node), h.parse(node), i.parse(node), j.parse(node), node.tags()));
    }
}
