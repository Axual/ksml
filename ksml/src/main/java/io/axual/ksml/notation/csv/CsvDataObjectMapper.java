package io.axual.ksml.notation.csv;

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

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvException;
import io.axual.ksml.data.mapper.DataObjectMapper;
import io.axual.ksml.data.object.DataList;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.execution.FatalError;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;

import static io.axual.ksml.notation.csv.CsvNotation.DEFAULT_TYPE;
import static io.axual.ksml.notation.csv.CsvNotation.LINE_TYPE;

public class CsvDataObjectMapper implements DataObjectMapper<String> {
    private static final String[] EMPTY_LINE = new String[0];

    @Override
    public DataList toDataObject(DataType expected, String value) {
        var in = new StringReader(value);
        var reader = new CSVReader(in);
        try {
            var lines = reader.readAll();
            var result = new DataList(DEFAULT_TYPE);
            for (var line : lines) result.add(convertLine(line));
            return result;
        } catch (IOException | CsvException e) {
            throw FatalError.executionError("Could not parse CSV", e);
        }
    }

    private DataList convertLine(String[] elements) {
        var result = new DataList(LINE_TYPE);
        for (String element : elements) {
            result.add(DataString.from(element));
        }
        return result;
    }

    @Override
    public String fromDataObject(DataObject value) {
        var csv = new ArrayList<String[]>();
        if (value instanceof DataList valueList) {
            for (var line : valueList) csv.add(compileLine(line));
            var out = new StringWriter();
            var writer = new CSVWriter(out);
            writer.writeAll(csv);
            return out.toString();
        }
        return null;
    }

    private String[] compileLine(DataObject line) {
        if (line instanceof DataList lineList) {
            var result = new String[lineList.size()];
            for (int index = 0; index < lineList.size(); index++) {
                var item = lineList.get(index);
                result[index] = item != null ? item.toString() : null;
            }
            return result;
        }
        return EMPTY_LINE;
    }
}
