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

import io.axual.ksml.example.SensorData;
import io.axual.ksml.example.SensorType;

import java.util.Random;

public class SensorDataGenerator {
    private static final Random rand = new Random();

    private SensorDataGenerator() {
    }

    public static SensorData generateValue(String sensorName) {
        SensorData.Builder builder = SensorData.newBuilder()
                .setName(sensorName)
                .setTimestamp(System.currentTimeMillis());
        builder = generateType(builder);
        builder = generateColor(builder);
        builder = generateCity(builder);
        builder = generateOwner(builder);
        return builder.build();
    }

    private static int random(int range) {
        return random(0, range);
    }

    private static int random(int low, int high) {
        return rand.nextInt(low, high);
    }

    private static SensorData.Builder generateType(SensorData.Builder builder) {
        switch (random(5)) {
            case 0:
                return generateArea(builder);
            case 1:
                return generateHumidity(builder);
            case 2:
                return generateLength(builder);
            case 3:
                return generateState(builder);
            case 4:
            default:
                return generateTemperature(builder);
        }
    }

    private static SensorData.Builder generateArea(SensorData.Builder builder) {
        return builder
                .setType(SensorType.AREA)
                .setUnit(random(2) < 1 ? "m2" : "ft2")
                .setValue("" + random(1000));
    }

    private static SensorData.Builder generateHumidity(SensorData.Builder builder) {
        builder.setType(SensorType.HUMIDITY);

        if (rand.nextBoolean()) {
            // g/m3
            return builder
                    .setType(SensorType.HUMIDITY)
                    .setUnit("g/m3")
                    .setValue("" + random(100));
        } else {
            return builder
                    .setType(SensorType.HUMIDITY)
                    .setUnit("%")
                    .setValue("" + random(60, 80));
        }

    }

    private static SensorData.Builder generateLength(SensorData.Builder builder) {
        return builder
                .setType(SensorType.LENGTH)
                .setUnit(random(2) < 1 ? "m" : "ft")
                .setValue("" + random(1000));
    }

    private static SensorData.Builder generateState(SensorData.Builder builder) {
        return builder
                .setType(SensorType.STATE)
                .setUnit("state")
                .setValue(random(2) < 1 ? "off" : "on");
    }

    private static SensorData.Builder generateTemperature(SensorData.Builder builder) {
        builder.setType(SensorType.TEMPERATURE);
        if (rand.nextBoolean()) {
            return builder
                    .setUnit("C")
                    .setValue("" + random(-10, 35));
        } else {
            return builder
                    .setUnit("F")
                    .setValue("" + random(14, 95));
        }
    }

    private static SensorData.Builder generateColor(SensorData.Builder builder) {
        return builder.setColor(COLORS[random(COLORS.length)]);
    }

    private static SensorData.Builder generateCity(SensorData.Builder builder) {
        return builder.setCity(CITIES[random(CITIES.length)]);
    }

    private static SensorData.Builder generateOwner(SensorData.Builder builder) {
        return builder.setOwner(OWNERS[random(OWNERS.length)]);
    }

    public static final String[] COLORS = new String[]{"black", "blue", "red", "yellow", "white"};
    public static final String[] OWNERS = new String[]{"Alice", "Bob", "Charlie", "Dave", "Evan"};
    public static final String[] CITIES = new String[]{"Amsterdam", "Xanten", "Utrecht", "Alkmaar", "Leiden"};
}
