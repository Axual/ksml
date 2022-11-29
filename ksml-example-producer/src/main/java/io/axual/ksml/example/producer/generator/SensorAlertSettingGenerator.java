package io.axual.ksml.example.producer.generator;

/*-
 * ========================LICENSE_START=================================
 * KSML Example Producer
 * %%
 * Copyright (C) 2021 Axual B.V.
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

import java.util.ArrayList;

import io.axual.ksml.example.SensorAlertSetting;
import io.axual.ksml.example.SensorAlertSettings;
import io.axual.ksml.example.SensorType;

public class SensorAlertSettingGenerator {
    private SensorAlertSettingGenerator() {
    }

    public static SensorAlertSettings generateAlertSettings(String city) {
        var builder = SensorAlertSettings.newBuilder()
                .setCity(city);
        var alertSettings = new ArrayList<SensorAlertSetting>();
        switch (city) {
            case "Amsterdam", "Utrecht" -> {
                alertSettings.add(generateHumidity(city, "74", "68"));
                alertSettings.add(generateTemperature(city, "68", "41", "F"));
            }
            case "Alkmaar", "Leiden" -> {
                alertSettings.add(generateHumidity(city, "72", "70"));
                alertSettings.add(generateTemperature(city, "25", "0", "C"));
            }
            default -> {
                // No alerts set for other cities
            }
        }

        builder.setAlertSettings(alertSettings);
        return builder.build();
    }

    public static SensorAlertSetting generateHumidity(String city, String upperBoundary, String lowerBoundary) {
        return SensorAlertSetting.newBuilder()
                .setName("Humidity alert for " + city)
                .setType(SensorType.HUMIDITY)
                .setAlertAbove(upperBoundary)
                .setAlertBelow(lowerBoundary)
                .setUnit("%")
                .build();
    }

    public static SensorAlertSetting generateTemperature(String city, String upperBoundary, String lowerBoundary, String unit) {
        return SensorAlertSetting.newBuilder()
                .setName("Temperature(" + unit + ") alert for " + city)
                .setType(SensorType.TEMPERATURE)
                .setAlertAbove(upperBoundary)
                .setAlertBelow(lowerBoundary)
                .setUnit(unit)
                .build();
    }
}
