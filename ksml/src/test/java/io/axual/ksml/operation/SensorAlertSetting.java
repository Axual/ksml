package io.axual.ksml.operation;

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
import lombok.Singular;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.util.List;

@Builder
@Slf4j
public class SensorAlertSetting {

    static Schema ALERT_SETTINGS_SCHEMA;
    static Schema ALERT_SETTING_SCHEMA;
    static Schema ALERT_TYPE_SCHEMA;

    public static final String SCHEMA_LOCATION = "schemas/SensorAlertSettings.avsc";

    static {
        try {
            ALERT_SETTINGS_SCHEMA = new Schema.Parser().parse(ClassLoader.getSystemResourceAsStream(SCHEMA_LOCATION));
            ALERT_SETTING_SCHEMA = ALERT_SETTINGS_SCHEMA.getField("alertSettings").schema();
            ALERT_TYPE_SCHEMA = ALERT_SETTING_SCHEMA.getElementType().getField("type").schema();
        } catch (IOException e) {
            log.error("Failed to load schemas", e);
        }
    }

    String city;
    @Singular
    List<AlertSetting> alertSettings;

    @Builder
    public static class AlertSetting {
        String name;
        String alertBelow;
        String alertAbove;
        String unit;
        Type type;

        enum Type {AREA, HUMIDITY, LENGTH, STATE, TEMPERATURE}

        public GenericRecord toRecord() {
            GenericData.Record data = new GenericData.Record(ALERT_SETTING_SCHEMA.getElementType());
            data.put("name", name == null ? "NOT SET" : name);
            data.put("alertBelow", alertBelow == null ? "NOT SET" : alertBelow);
            data.put("alertAbove", alertAbove == null ? "NOT SET" : alertAbove);
            data.put("unit", unit == null ? "NOT SET" : unit);
            data.put("type", new GenericData.EnumSymbol(ALERT_TYPE_SCHEMA, type == null ? Type.AREA : type));
            return data;
        }
    }

    public GenericRecord toRecord() {
        GenericRecord data = new GenericData.Record(ALERT_SETTINGS_SCHEMA);
        data.put("city", city == null ? "NOT SET" : city);
        GenericArray settings = new GenericData.Array(alertSettings.size(), ALERT_SETTING_SCHEMA);
        for (AlertSetting setting : alertSettings) {
            settings.add(setting.toRecord());
        }
        data.put("alertSettings", settings);
        return data;
    }
}
