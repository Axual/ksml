package io.axual.ksml.store;

import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.definition.StateStoreDefinition;
import io.axual.ksml.definition.TopologyResource;
import io.axual.ksml.definition.parser.StateStoreDefinitionParser;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.exception.ParseException;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.parser.ParseNode;
import io.axual.ksml.parser.StructsParser;
import io.axual.ksml.parser.TopologyResourceParser;
import io.axual.ksml.util.ParserUtil;

import java.util.List;

public class StoreParserUtil {
    private StoreParserUtil() {
    }

    public static StructsParser<StateStoreDefinition> storeField(String childName, boolean required, String doc, StoreType expectedStoreType, TopologyResources resources) {
        final var stateStoreParser = new StateStoreDefinitionParser(expectedStoreType, false);
        final var resourceParser = new TopologyResourceParser<>("state store", childName, doc, (name, context) -> resources.stateStore(name), stateStoreParser);
        final var schemas = required ? resourceParser.schemas() : ParserUtil.optional(resourceParser).schemas();
        return new StructsParser<>() {
            @Override
            public StateStoreDefinition parse(ParseNode node) {
                stateStoreParser.defaultShortName(node.name());
                stateStoreParser.defaultLongName(node.longName());
                final var resource = resourceParser.parse(node);
                if (resource != null && resource.definition() instanceof StateStoreDefinition def) return def;
                if (!required) return null;
                throw new ParseException(node, "Required state store is not defined");
            }

            @Override
            public List<StructSchema> schemas() {
                return schemas;
            }
        };
    }
}
