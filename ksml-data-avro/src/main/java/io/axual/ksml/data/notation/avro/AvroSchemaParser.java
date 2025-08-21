package io.axual.ksml.data.notation.avro;

/*-
 * ========================LICENSE_START=================================
 * KSML
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

import io.axual.ksml.data.exception.SchemaException;
import io.axual.ksml.data.notation.Notation;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.StructSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;

/**
 * Parses Avro schema JSON strings (.avsc) into KSML DataSchema instances.
 *
 * <p>Ensures the top-level Avro schema maps to a StructSchema, throwing SchemaException otherwise.</p>
 */
@Slf4j
public class AvroSchemaParser implements Notation.SchemaParser {
    private static final AvroSchemaMapper MAPPER = new AvroSchemaMapper();

    /**
     * Parse a textual Avro schema into KSML DataSchema.
     *
     * @param contextName a logical context identifier (e.g., vendor subject/context); not used by this parser
     * @param schemaName  the schema name used for mapping and validation
     * @param schemaString the Avro schema JSON text (.avsc content)
     * @return a StructSchema converted from the Avro schema
     * @throws SchemaException when the result is not a StructSchema
     */
    @Override
    public DataSchema parse(String contextName, String schemaName, String schemaString) {
        final var parsedSchema = new Schema.Parser().parse(schemaString);
        final var result = MAPPER.toDataSchema(schemaName, parsedSchema);
        if (!(result instanceof StructSchema)) {
            throw new SchemaException("AVRO schema did not return a StructSchema");
        }
        return result;
    }
}
