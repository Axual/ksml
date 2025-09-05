package io.axual.ksml.data.notation.json;

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

import io.axual.ksml.data.notation.Notation;
import io.axual.ksml.data.schema.DataSchema;
import lombok.extern.slf4j.Slf4j;

/**
 * Loads/parses JSON Schema text into KSML {@link DataSchema} using {@link JsonSchemaMapper}.
 *
 * <p>This class implements {@link Notation.SchemaParser} for the JSON notation. It delegates all
 * parsing work to {@link JsonSchemaMapper} while keeping the notation SPI interface clean.</p>
 *
 * <p>Notes on parameters:</p>
 * <ul>
 *   <li>{@code contextName}: a human-friendly namespace or context; currently not used for parsing but
 *       kept for future diagnostics or resolver strategies.</li>
 *   <li>{@code schemaName}: the logical name/title of the schema; passed to the mapper to label the schema.</li>
 *   <li>{@code schemaString}: the JSON Schema document as a string.</li>
 * </ul>
 */
@Slf4j
public class JsonSchemaLoader implements Notation.SchemaParser {
    /**
     * Mapper handling JSON Schema <-> DataSchema mapping.
     */
    private static final JsonSchemaMapper MAPPER = new JsonSchemaMapper(false);

    /**
     * Parses a JSON Schema string into a {@link DataSchema}.
     *
     * @param contextName a human-readable context or namespace (currently informational only)
     * @param schemaName  the logical name of the schema to parse
     * @param schemaString the JSON Schema document as a string
     * @return the parsed {@link DataSchema}
     * @throws io.axual.ksml.data.exception.DataException if the schema can not be parsed
     */
    @Override
    public DataSchema parse(String contextName, String schemaName, String schemaString) {
        // Delegate actual parsing to the dedicated mapper. The contextName is not used at the moment.
        return MAPPER.toDataSchema(schemaName, schemaString);
    }
}
