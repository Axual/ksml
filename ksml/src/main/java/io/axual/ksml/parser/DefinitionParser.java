package io.axual.ksml.parser;

import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.schema.DataField;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.DataValue;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.schema.UnionSchema;
import io.axual.ksml.data.type.UserType;
import io.axual.ksml.execution.FatalError;
import lombok.Getter;
import lombok.Setter;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class DefinitionParser<T> extends BaseParser<T> implements StructParser<T> {
    public static final String SCHEMA_NAMESPACE = "io.axual.ksml";
    // The global namespace for this definition parser. The namespace is the specified name in the runner configuration
    // for this particular KSML definition. It is used to prefix global names in Kafka Streams (eg. processor names) to
    // ensure no conflict between processor names for two separate definitions.
    private final String namespace;
    private StructParser<T> parser;

    public DefinitionParser() {
        this(null);
    }

    public DefinitionParser(String namespace) {
        this.namespace = namespace;
    }

    protected String namespace() {
        if (namespace != null) return namespace;
        throw FatalError.topologyError("Topology namespace not properly initialized. This is a programming error.");
    }

    protected abstract StructParser<T> parser();

    public final T parse(YamlNode node) {
        if (parser == null) parser = parser();
        return parser.parse(node);
    }

    public final StructSchema schema() {
        if (parser == null) parser = parser();
        return parser.schema();
    }

    protected static <V> StructParser<V> optional(StructParser<V> parser) {
        if (parser.fields().isEmpty()) return parser;
        final var schema = parser.schema();
        final var newFields = schema.fields().stream()
                .map(field -> new DataField(field.name(), new UnionSchema(DataSchema.nullSchema(), field.schema()), "(Optional) " + field.doc(), new DataValue(DataNull.INSTANCE)))
                .toList();
        final var newSchema = new StructSchema(schema.namespace(), schema.name(), schema.doc(), newFields);
        return StructParser.of(node -> node != null ? parser.parse(node) : null, newSchema);
    }

    protected static StructSchema structSchema(Class<?> clazz, String doc, List<DataField> fields) {
        return structSchema(clazz.getSimpleName(), doc, fields);
    }

    protected static StructSchema structSchema(String name, String doc, List<DataField> fields) {
        return new StructSchema(SCHEMA_NAMESPACE, name, doc, fields);
    }

    private static class FieldParser<V> implements StructParser<V>, NamedObjectParser {
        private final DataField field;
        @Getter
        private final StructSchema schema;
        private final Parser<V> valueParser;
        @Getter
        @Setter
        private String defaultName;

        public FieldParser(String childName, boolean mandatory, V valueIfNull, String doc, ParserWithSchema<V> valueParser) {
            field = new DataField(childName, mandatory ? valueParser.schema() : new UnionSchema(DataSchema.nullSchema(), valueParser.schema()), doc, valueIfNull != null ? new DataValue(valueIfNull) : null);
            schema = structSchema((String) null, null, List.of(field));
            this.valueParser = valueParser;
        }

        @Override
        public V parse(YamlNode node) {
            try {
                if (node == null) return null;
                final var child = node.get(field.name());
                return child != null ? valueParser.parse(child) : null;
            } catch (Exception e) {
                throw FatalError.parseError(node, e.getMessage());
            }
        }
    }

    private static class ValueStructParser<V> implements StructParser<V> {
        @Getter
        private final StructSchema schema;
        private final Parser<V> valueParser;

        public ValueStructParser(String name, String doc, List<StructParser<?>> fields, Parser<V> valueParser) {
            final var structFields = new ArrayList<DataField>();
            fields.forEach(p -> structFields.addAll(p.fields()));
            schema = structSchema(name, doc, structFields);
            this.valueParser = valueParser;
        }

        @Override
        public V parse(YamlNode node) {
            try {
                return valueParser.parse(node);
            } catch (Exception e) {
                throw FatalError.parseError(node, e.getMessage());
            }
        }
    }

    protected <V> StructParser<V> freeField(String childName, boolean mandatory, V valueIfNull, String doc, ParserWithSchema<V> parser) {
        return new FieldParser<>(childName, mandatory, valueIfNull, doc, parser);
    }

    protected StructParser<Boolean> booleanField(String childName, boolean mandatory, String doc) {
        return booleanField(childName, mandatory, false, doc);
    }

    protected StructParser<Boolean> booleanField(String childName, boolean mandatory, boolean valueIfNull, String doc) {
        return freeField(childName, mandatory, valueIfNull, doc, ParserWithSchema.of(YamlNode::asBoolean, DataSchema.booleanSchema()));
    }

    protected StructParser<Duration> durationField(String childName, boolean mandatory, String doc) {
        final var stringParser = stringField(childName, mandatory, null, doc);
        return StructParser.of(node -> parseDuration(stringParser.parse(node)), stringParser.schema());
    }

    protected StructParser<Integer> integerField(String childName, boolean mandatory, String doc) {
        return integerField(childName, mandatory, null, doc);
    }

    protected StructParser<Integer> integerField(String childName, boolean mandatory, Integer valueIfNull, String doc) {
        return freeField(childName, mandatory, valueIfNull, doc, ParserWithSchema.of(YamlNode::asInt, DataSchema.integerSchema()));
    }

    protected StructParser<String> stringField(String childName, boolean mandatory, String doc) {
        return stringField(childName, mandatory, null, doc);
    }

    protected StructParser<String> stringField(String childName, boolean mandatory, String valueIfNull, String doc) {
        return freeField(childName, mandatory, valueIfNull, doc, stringValueParser);
    }

    protected StructParser<String> codeField(String childName, boolean mandatory, String doc) {
        return codeField(childName, mandatory, null, doc);
    }

    protected StructParser<String> codeField(String childName, boolean mandatory, String valueIfNull, String doc) {
        return freeField(childName, mandatory, valueIfNull, doc, codeParser);
    }

    protected StructParser<UserType> userTypeField(String childName, boolean mandatory, String doc) {
        final var field = new DataField(childName, UserTypeParser.getSchema(), doc, null);
        final var stringParser = stringField(childName, mandatory, null, doc);
        return new StructParser<>() {
            @Override
            public StructSchema schema() {
                return structSchema((String) null, null, List.of(field));
            }

            @Override
            public UserType parse(YamlNode node) {
                final var type = UserTypeParser.parse(stringParser.parse(node));
                return type != null ? type : UserType.UNKNOWN;
            }
        };
    }

    protected <TYPE> StructParser<List<TYPE>> listField(String childName, String whatToParse, boolean mandatory, String doc, ParserWithSchema<TYPE> valueParser) {
        return new FieldParser<>(childName, mandatory, new ArrayList<>(), doc, new ListWithSchemaParser<>(whatToParse, valueParser));
    }

    protected <TYPE> StructParser<Map<String, TYPE>> mapField(String childName, String whatToParse, boolean mandatory, String doc, ParserWithSchema<TYPE> valueParser) {
        return new FieldParser<>(childName, mandatory, new HashMap<>(), doc, new MapWithSchemaParser<>(whatToParse, valueParser));
    }

    protected <TYPE> StructParser<TYPE> customField(String childName, boolean mandatory, String doc, StructParser<TYPE> valueParser) {
        return new FieldParser<>(childName, mandatory, null, doc, valueParser);
    }

    public interface Constructor0<RESULT> {
        RESULT construct();
    }

    public interface Constructor1<RESULT, A> {
        RESULT construct(A a);
    }

    public interface Constructor2<RESULT, A, B> {
        RESULT construct(A a, B b);
    }

    public interface Constructor3<RESULT, A, B, C> {
        RESULT construct(A a, B b, C c);
    }

    public interface Constructor4<RESULT, A, B, C, D> {
        RESULT construct(A a, B b, C c, D d);
    }

    public interface Constructor5<RESULT, A, B, C, D, E> {
        RESULT construct(A a, B b, C c, D d, E e);
    }

    public interface Constructor6<RESULT, A, B, C, D, E, F> {
        RESULT construct(A a, B b, C c, D d, E e, F f);
    }

    public interface Constructor7<RESULT, A, B, C, D, E, F, G> {
        RESULT construct(A a, B b, C c, D d, E e, F f, G g);
    }

    public interface Constructor8<RESULT, A, B, C, D, E, F, G, H> {
        RESULT construct(A a, B b, C c, D d, E e, F f, G g, H h);
    }

    public interface Constructor9<RESULT, A, B, C, D, E, F, G, H, I> {
        RESULT construct(A a, B b, C c, D d, E e, F f, G g, H h, I i);
    }

    public interface Constructor10<RESULT, A, B, C, D, E, F, G, H, I, J> {
        RESULT construct(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j);
    }

    public interface Constructor11<RESULT, A, B, C, D, E, F, G, H, I, J, K> {
        RESULT construct(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, K k);
    }

    protected <TYPE> StructParser<TYPE> structParser(Class<TYPE> resultClass, String doc, Constructor0<TYPE> constructor) {
        return new ValueStructParser<>(resultClass.getSimpleName(), doc, List.of(), node -> constructor.construct());
    }

    protected <TYPE, A> StructParser<TYPE> structParser(Class<TYPE> resultClass, String doc, StructParser<A> a, Constructor1<TYPE, A> constructor) {
        return new ValueStructParser<>(resultClass.getSimpleName(), doc, List.of(a), node -> {
            final var parsedA = a.parse(node);
            return constructor.construct(parsedA);
        });
    }

    protected <TYPE, A, B> StructParser<TYPE> structParser(Class<TYPE> resultClass, String doc, StructParser<A> a, StructParser<B> b, Constructor2<TYPE, A, B> constructor) {
        return new ValueStructParser<>(resultClass.getSimpleName(), doc, List.of(a, b), node -> constructor.construct(a.parse(node), b.parse(node)));
    }

    protected <TYPE, A, B, C> StructParser<TYPE> structParser(Class<TYPE> resultClass, String doc, StructParser<A> a, StructParser<B> b, StructParser<C> c, Constructor3<TYPE, A, B, C> constructor) {
        return new ValueStructParser<>(resultClass.getSimpleName(), doc, List.of(a, b, c), node -> constructor.construct(a.parse(node), b.parse(node), c.parse(node)));
    }

    protected <TYPE, A, B, C, D, E, F, G> StructParser<TYPE> structParser(Class<TYPE> resultClass, String doc, StructParser<A> a, StructParser<B> b, StructParser<C> c, StructParser<D> d, Constructor4<TYPE, A, B, C, D> constructor) {
        return new ValueStructParser<>(resultClass.getSimpleName(), doc, List.of(a, b, c, d), node -> constructor.construct(a.parse(node), b.parse(node), c.parse(node), d.parse(node)));
    }

    protected <TYPE, A, B, C, D, E, F, G> StructParser<TYPE> structParser(Class<TYPE> resultClass, String doc, StructParser<A> a, StructParser<B> b, StructParser<C> c, StructParser<D> d, StructParser<E> e, Constructor5<TYPE, A, B, C, D, E> constructor) {
        return new ValueStructParser<>(resultClass.getSimpleName(), doc, List.of(a, b, c, d, e), node -> constructor.construct(a.parse(node), b.parse(node), c.parse(node), d.parse(node), e.parse(node)));
    }

    protected <TYPE, A, B, C, D, E, F, G> StructParser<TYPE> structParser(Class<TYPE> resultClass, String doc, StructParser<A> a, StructParser<B> b, StructParser<C> c, StructParser<D> d, StructParser<E> e, StructParser<F> f, Constructor6<TYPE, A, B, C, D, E, F> constructor) {
        return new ValueStructParser<>(resultClass.getSimpleName(), doc, List.of(a, b, c, d, e, f), node -> constructor.construct(a.parse(node), b.parse(node), c.parse(node), d.parse(node), e.parse(node), f.parse(node)));
    }

    protected <TYPE, A, B, C, D, E, F, G> StructParser<TYPE> structParser(Class<TYPE> resultClass, String doc, StructParser<A> a, StructParser<B> b, StructParser<C> c, StructParser<D> d, StructParser<E> e, StructParser<F> f, StructParser<G> g, Constructor7<TYPE, A, B, C, D, E, F, G> constructor) {
        return new ValueStructParser<>(resultClass.getSimpleName(), doc, List.of(a, b, c, d, e, f, g), node -> constructor.construct(a.parse(node), b.parse(node), c.parse(node), d.parse(node), e.parse(node), f.parse(node), g.parse(node)));
    }

    protected <TYPE, A, B, C, D, E, F, G, H> StructParser<TYPE> structParser(Class<TYPE> resultClass, String doc, StructParser<A> a, StructParser<B> b, StructParser<C> c, StructParser<D> d, StructParser<E> e, StructParser<F> f, StructParser<G> g, StructParser<H> h, Constructor8<TYPE, A, B, C, D, E, F, G, H> constructor) {
        return new ValueStructParser<>(resultClass.getSimpleName(), doc, List.of(a, b, c, d, e, f, g, h), node -> constructor.construct(a.parse(node), b.parse(node), c.parse(node), d.parse(node), e.parse(node), f.parse(node), g.parse(node), h.parse(node)));
    }

    protected <TYPE, A, B, C, D, E, F, G, H, I> StructParser<TYPE> structParser(Class<TYPE> resultClass, String doc, StructParser<A> a, StructParser<B> b, StructParser<C> c, StructParser<D> d, StructParser<E> e, StructParser<F> f, StructParser<G> g, StructParser<H> h, StructParser<I> i, Constructor9<TYPE, A, B, C, D, E, F, G, H, I> constructor) {
        return new ValueStructParser<>(resultClass.getSimpleName(), doc, List.of(a, b, c, d, e, f, g, h, i), node -> constructor.construct(a.parse(node), b.parse(node), c.parse(node), d.parse(node), e.parse(node), f.parse(node), g.parse(node), h.parse(node), i.parse(node)));
    }

    protected <TYPE, A, B, C, D, E, F, G, H, I, J> StructParser<TYPE> structParser(Class<TYPE> resultClass, String doc, StructParser<A> a, StructParser<B> b, StructParser<C> c, StructParser<D> d, StructParser<E> e, StructParser<F> f, StructParser<G> g, StructParser<H> h, StructParser<I> i, StructParser<J> j, Constructor10<TYPE, A, B, C, D, E, F, G, H, I, J> constructor) {
        return new ValueStructParser<>(resultClass.getSimpleName(), doc, List.of(a, b, c, d, e, f, g, h, i, j), node -> constructor.construct(a.parse(node), b.parse(node), c.parse(node), d.parse(node), e.parse(node), f.parse(node), g.parse(node), h.parse(node), i.parse(node), j.parse(node)));
    }

    protected <TYPE, A, B, C, D, E, F, G, H, I, J, K> StructParser<TYPE> structParser(Class<TYPE> resultClass, String doc, StructParser<A> a, StructParser<B> b, StructParser<C> c, StructParser<D> d, StructParser<E> e, StructParser<F> f, StructParser<G> g, StructParser<H> h, StructParser<I> i, StructParser<J> j, StructParser<K> k, Constructor11<TYPE, A, B, C, D, E, F, G, H, I, J, K> constructor) {
        return new ValueStructParser<>(resultClass.getSimpleName(), doc, List.of(a, b, c, d, e, f, g, h, i, j, k), node -> constructor.construct(a.parse(node), b.parse(node), c.parse(node), d.parse(node), e.parse(node), f.parse(node), g.parse(node), h.parse(node), i.parse(node), j.parse(node), k.parse(node)));
    }
}
