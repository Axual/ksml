package io.axual.ksml;

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

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;

import static io.axual.ksml.SensorData.SensorType.AREA;

/**
 * Test utility class to create GenericRecords matching the SensorData.avsc schema.
 */
@Builder
@Slf4j
class SensorData {

    static Schema SENSOR_DATA_SCHEMA;

    static Schema SENSOR_TYPE_SCHEMA;

    public static final String SCHEMA_LOCATION = "pipelines/SensorData.avsc";

    static {
        try {
            SENSOR_DATA_SCHEMA = new Schema.Parser().parse(ClassLoader.getSystemResourceAsStream(SCHEMA_LOCATION));
            SENSOR_TYPE_SCHEMA = SENSOR_DATA_SCHEMA.getField("type").schema();
        } catch (IOException e) {
            log.error("Failed to load schemas", e);
        }
    }

    String name;
    long timestamp;
    String value;
    SensorType type;
    String unit;
    String color;
    String city;

    enum SensorType {
        AREA, HUMIDITY, LENGTH, STATE, TEMPERATURE
    }

    public GenericRecord toRecord() {
        GenericRecord data = new GenericData.Record(SENSOR_DATA_SCHEMA);
        data.put("name", name == null ? "NOT SET" : name);
        data.put("timestamp", timestamp);
        data.put("value", value == null ? "NOT SET" : value);
        data.put("type", new GenericData.EnumSymbol(SENSOR_TYPE_SCHEMA, type == null ? AREA : type));
        data.put("unit", unit == null ? "NOT SET" : unit);
        data.put("color", color == null ? "NOT SET" : color);
        data.put("city", city == null ? "NOT SET" : city);
        return data;
    }
}
