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

import java.util.Random;

import io.axual.ksml.example.SensorData;
import io.axual.ksml.example.SensorType;

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
        return rand.nextInt(range);
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
        return builder
                .setType(SensorType.HUMIDITY)
                .setUnit(random(2) < 1 ? "g/m3" : "%")
                .setValue("" + random(100));
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
        return builder
                .setType(SensorType.TEMPERATURE)
                .setUnit(random(2) < 1 ? "C" : "F")
                .setValue("" + random(1000));
    }

    private static SensorData.Builder generateColor(SensorData.Builder builder) {
        switch (random(5)) {
            case 0:
                return builder.setColor("black");
            case 1:
                return builder.setColor("blue");
            case 2:
                return builder.setColor("red");
            case 3:
                return builder.setColor("yellow");
            case 4:
            default:
                return builder.setColor("white");
        }
    }

    private static SensorData.Builder generateCity(SensorData.Builder builder) {
        switch (random(5)) {
            case 0:
                return builder.setCity("Amsterdam");
            case 1:
                return builder.setCity("Xanten");
            case 2:
                return builder.setCity("Utrecht");
            case 3:
                return builder.setCity("Alkmaar");
            case 4:
            default:
                return builder.setCity("Leiden");
        }
    }

    private static SensorData.Builder generateOwner(SensorData.Builder builder) {
        switch (random(5)) {
            case 0:
                return builder.setOwner("Alice");
            case 1:
                return builder.setOwner("Bob");
            case 2:
                return builder.setOwner("Charlie");
            case 3:
                return builder.setOwner("Dave");
            case 4:
            default:
                return builder.setOwner("Evan");
        }
    }
}
