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

import io.axual.ksml.data.schema.DataField;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.schema.UnionSchema;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.parser.StructsParser;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

import static io.axual.ksml.parser.DefinitionParser.SCHEMA_NAMESPACE;

@Slf4j
public class SchemaUtil {
    public static void addToSchemas(List<StructSchema> schemas, String name, String doc, StructsParser<?> subParser) {
        final var subSchemas = getSubSchemas(subParser.schema());
        if (schemas.isEmpty()) {
            schemas.addAll(subSchemas);
        } else {
            // Here we combine the schemas from the subParser with the known list of schemas. Effectively this
            // multiplies the number of schemas. So if we had 3 schemas in our schemas variable already, and the
            // parser also parses 4 schema alternatives, then we end up with 12 new schemas.
            final var newSchemas = new ArrayList<StructSchema>();
            for (final var schema : schemas) {
                final var usePostfix = subSchemas.size() > 1;
                for (final var subSchema : subParser.schemas()) {
                    final var fields = new ArrayList<DataField>();
                    fields.addAll(schema.fields());
                    fields.addAll(subSchema.fields());
                    final var newName = usePostfix ? name + KSMLDSL.Types.WITH_PREFIX + subSchema.name() : name;
                    final var newSchema = new StructSchema(SCHEMA_NAMESPACE, newName, doc, fields);
                    newSchemas.add(newSchema);
                }
            }

            // Clear the old schema array and replace with new schemas
            schemas.clear();
            schemas.addAll(newSchemas);
        }
    }

    private static List<StructSchema> getSubSchemas(DataSchema schema) {
        final var result = new ArrayList<StructSchema>();
        if (schema instanceof UnionSchema unionSchema) {
            for (final var valueType : unionSchema.valueTypes()) {
                if (valueType.schema() instanceof StructSchema structSchema) {
                    result.add(structSchema);
                } else {
                    log.warn("Could not convert union subtype: {}", valueType.schema().type());
                }
            }
        } else {
            if (schema instanceof StructSchema structSchema) {
                result.add(structSchema);
            } else {
                log.warn("Could not convert subtype: {}", schema.type());
            }
        }
        return result;
    }
}
