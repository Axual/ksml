package io.axual.ksml.data.notation.avro.test;

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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Objects;

/**
 * Test helper to load Avro schemas and JSON-encoded Avro data from test resources.
 */
public final class AvroTestUtil {
    /**
     * Base directory for Avro test resources.
     */
    public static final String AVRO_BASE = "avro/";
    /**
     * Directory containing Avro schema (.avsc) files.
     */
    public static final String SCHEMAS_DIR = AVRO_BASE + "schemas/";
    /**
     * Directory containing Avro JSON-encoded data files.
     */
    public static final String DATA_DIR = AVRO_BASE + "data/";

    // Schema resources
    /**
     * Avro schema for primitive types. Record io.axual.test.Primitives with string, int, long, float, double, boolean, bytes.
     */
    public static final String SCHEMA_PRIMITIVES = SCHEMAS_DIR + "primitives_record.avsc";
    /**
     * Avro schema for common logical types: date, time-millis, timestamp-millis, uuid, decimal(10,2).
     */
    public static final String SCHEMA_LOGICAL_TYPES = SCHEMAS_DIR + "logical_types_record.avsc";
    /**
     * Avro schema covering arrays, maps, and unions including nested list of unions with null|string|int|record X|enum Y.
     */
    public static final String SCHEMA_COLLECTIONS = SCHEMAS_DIR + "collections_record.avsc";
    /**
     * Avro schema covering different primitive arrays in a record.
     */
    public static final String SCHEMA_ARRAYS = SCHEMAS_DIR + "arrays_record.avsc";
    /**
     * Avro schema where all fields are optional via [null, T] unions, covering primitives and basic complex types.
     */
    public static final String SCHEMA_OPTIONAL = SCHEMAS_DIR + "optional_record.avsc";

    // Data resources
    /**
     * JSON Avro data for the Primitives schema with representative values.
     */
    public static final String DATA_PRIMITIVES = DATA_DIR + "primitives_record.json";
    /**
     * JSON Avro data for the LogicalTypes schema with example logical values.
     */
    public static final String DATA_LOGICAL_TYPES = DATA_DIR + "logical_types_record.json";
    /**
     * JSON Avro data for the Collections schema: arrays and maps populated; singleUnion is a string; unionList exercises all branches.
     */
    public static final String DATA_COLLECTIONS_1 = DATA_DIR + "collections_record_1.json";
    /**
     * JSON Avro data for the Arrays schema: arrays for all fields are populated.
     */
    public static final String DATA_ARRAYS_1 = DATA_DIR + "arrays_record_1.json";
    /**
     * JSON Avro data for the Collections schema with singleUnion set to null and empty collections.
     */
    public static final String DATA_COLLECTIONS_SINGLE_UNION_NULL = DATA_DIR + "collections_record_singleUnion_null.json";
    /**
     * JSON Avro data for the Collections schema with singleUnion set to an int value.
     */
    public static final String DATA_COLLECTIONS_SINGLE_UNION_INT = DATA_DIR + "collections_record_singleUnion_int.json";
    /**
     * JSON Avro data for the Collections schema with singleUnion set to record io.axual.test.X.
     */
    public static final String DATA_COLLECTIONS_SINGLE_UNION_RECORD = DATA_DIR + "collections_record_singleUnion_record.json";

    /**
     * JSON Avro data for OptionalFields schema with representative non-null values for each field.
     */
    public static final String DATA_OPTIONAL_WITH_VALUES = DATA_DIR + "optional_record_with_values.json";

    private AvroTestUtil() {}

    /**
     * Load an Avro Schema object from a classpath resource.
     *
     * @param resourcePath classpath-relative resource path to a .avsc file (e.g. {@link #SCHEMA_PRIMITIVES})
     * @return parsed Avro Schema instance
     * @throws RuntimeException if the resource cannot be found or parsed
     */
    public static Schema loadSchema(String resourcePath) {
        String schemaString = loadResourceToString(resourcePath);
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(schemaString);
    }

    /**
     * Load the string content of a classpath resource.
     *
     * @param resourcePath classpath-relative resource path to a file
     * @return String content of the resource
     * @throws RuntimeException if the resource cannot be found
     */
    public static String loadResourceToString(String resourcePath) {
        try (InputStream is = openResource(resourcePath)) {
            if (is == null)
                throw new IllegalArgumentException("Resource not found: " + resourcePath);
            return readToString(is);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load resource: " + resourcePath, e);
        }
    }

    /**
     * Parse a single JSON-encoded Avro record from a resource using the provided schema.
     *
     * The JSON must follow Avro's JSON encoding rules for the given schema.
     *
     * @param schema            Avro schema that describes the record structure
     * @param jsonResourcePath  classpath-relative resource path to a JSON Avro file (e.g. {@link #DATA_PRIMITIVES})
     * @return a GenericRecord instance read from the JSON
     * @throws RuntimeException if the resource cannot be found or parsed
     */
    public static GenericRecord parseRecord(Schema schema, String jsonResourcePath) {
        try (InputStream is = openResource(jsonResourcePath)) {
            if (is == null) throw new IllegalArgumentException("Data resource not found: " + jsonResourcePath);
            String json = readToString(is);
            Decoder decoder = DecoderFactory.get().jsonDecoder(schema, json);
            GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
            return reader.read(null, decoder);
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse JSON data: " + jsonResourcePath, e);
        }
    }

    /**
     * Parse all JSON Avro files (ending with .json) in a given classpath directory into GenericRecord objects.
     *
     * @param schema           Avro schema used to decode all JSON files
     * @param dirResourcePath  classpath-relative directory path (e.g. {@link #DATA_DIR})
     * @return list of GenericRecord objects, in undefined order depending on classloader file listing
     */
    public static List<GenericRecord> parseAllRecordsInDir(Schema schema, String dirResourcePath) {
        List<GenericRecord> result = new ArrayList<>();
        for (String path : listResourceFiles(dirResourcePath)) {
            if (path.endsWith(".json")) {
                result.add(parseRecord(schema, path));
            }
        }
        return result;
    }

    /**
     * Open a classpath resource as an InputStream.
     *
     * This method first uses the thread context ClassLoader and then the class' ClassLoader as a fallback.
     * The provided path may start with a leading slash or be relative (both are supported).
     *
     * @param resourcePath classpath-relative path to the resource
     * @return InputStream to the resource, or null if not found
     */
    public static InputStream openResource(String resourcePath) {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        InputStream is = cl.getResourceAsStream(resourcePath.startsWith("/") ? resourcePath.substring(1) : resourcePath);
        if (is != null) return is;
        // Try without leading folder in case caller provides relative like "avro/data/file.json"
        return AvroTestUtil.class.getClassLoader().getResourceAsStream(resourcePath);
    }

    /**
     * List files directly under a classpath directory (non-recursive).
     *
     * Only file URLs are supported (typical for running tests from the filesystem). Returned entries are
     * classpath-relative paths constructed by concatenating the directory path and file name.
     *
     * @param dirResourcePath classpath-relative directory path ending with or without '/'
     * @return list of file resource paths under the given directory; empty if none or on error
     */
    public static List<String> listResourceFiles(String dirResourcePath) {
        String normalized = dirResourcePath;
        if (normalized.startsWith("/")) normalized = normalized.substring(1);
        if (!normalized.endsWith("/")) normalized += "/";
        List<String> files = new ArrayList<>();
        try {
            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            Enumeration<URL> dirs = cl.getResources(normalized);
            while (dirs.hasMoreElements()) {
                URL url = dirs.nextElement();
                if (Objects.equals(url.getProtocol(), "file")) {
                    File dir = new File(url.getPath());
                    if (dir.isDirectory()) {
                        File[] children = dir.listFiles();
                        if (children != null) {
                            for (File child : children) {
                                if (child.isFile()) {
                                    files.add(normalized + child.getName());
                                }
                            }
                        }
                    }
                }
            }
        } catch (IOException ignored) {
            // Fall through
        }
        return files;
    }

    /**
     * Read the entire content of an InputStream into a UTF-8 string.
     *
     * @param is input stream to read
     * @return string content
     * @throws IOException if reading fails
     */
    private static String readToString(InputStream is) throws IOException {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line).append('\n');
            }
            return sb.toString();
        }
    }
}
