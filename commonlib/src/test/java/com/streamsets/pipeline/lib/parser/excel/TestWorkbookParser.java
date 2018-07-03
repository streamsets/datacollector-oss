/*
 * Copyright 2018 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file
 */

package com.streamsets.pipeline.lib.parser.excel;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.config.ExcelHeader;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.RecoverableDataParserException;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestWorkbookParser {
  WorkbookParserSettings settingsNoHeader;
  WorkbookParserSettings settingsWithHeader;
  WorkbookParserSettings settingsIgnoreHeader;

  @Rule
  public final ExpectedException exception = ExpectedException.none();

  private InputStream getFile(String path) {
    return ClassLoader.class.getResourceAsStream(path);
  }

  private Workbook createWorkbook(String filePath) throws IOException, InvalidFormatException {
    return WorkbookFactory.create(getFile(filePath));
  }

  private Stage.Context getContext() {
    return ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR, Collections.EMPTY_LIST);
  }

  @Before
  public void setUp() throws Exception {
    settingsNoHeader = WorkbookParserSettings.builder()
        .withHeader(ExcelHeader.NO_HEADER)
        .build();

    settingsWithHeader = WorkbookParserSettings.builder()
        .withHeader(ExcelHeader.WITH_HEADER)
        .build();

    settingsIgnoreHeader = WorkbookParserSettings.builder()
        .withHeader(ExcelHeader.IGNORE_HEADER)
        .build();
  }

  @Test
  public void testParseCorrectlyReturnsCachedValueOfFormula() throws IOException, InvalidFormatException, DataParserException {
    Workbook workbook = createWorkbook("/excel/TestFormulas.xlsx");

    WorkbookParser parser = new WorkbookParser(settingsNoHeader, getContext(), workbook, "Sheet1::0");

    Record recordFirstRow = parser.parse();
    Record recordSecondRow = parser.parse();

    LinkedHashMap<String, Field> firstMap = new LinkedHashMap<>();
    firstMap.put("0", Field.create("Addition"));
    firstMap.put("1", Field.create("Division"));
    firstMap.put("2", Field.create("Neighbor Multiplication"));
    Field expectedFirstRow = Field.createListMap(firstMap);

    LinkedHashMap<String, Field> secondMap = new LinkedHashMap<>();
    secondMap.put("0", Field.create(new BigDecimal(8.0).setScale(1)));
    secondMap.put("1", Field.create(new BigDecimal(9.0).setScale(1)));
    secondMap.put("2", Field.create(new BigDecimal(72.0).setScale(1)));
    Field expectedSecondRow = Field.createListMap(secondMap);

    assertEquals(expectedFirstRow, recordFirstRow.get());
    assertEquals(expectedSecondRow, recordSecondRow.get());
  }

  @Test
  public void testParseCorrectlyHandlesFilesWithHeaders() throws IOException, InvalidFormatException, DataParserException {
    Workbook workbook = createWorkbook("/excel/TestExcel.xlsx");

    WorkbookParser parser = new WorkbookParser(settingsWithHeader, getContext(), workbook, "Sheet1::0");

    Record firstContentRow = parser.parse();

    LinkedHashMap<String, Field> contentMap = new LinkedHashMap<>();
    for (int i = 1; i <= 5; i++) {
      contentMap.put("column" + i, Field.create(new BigDecimal(i)));
    }
    Field expected = Field.createListMap(contentMap);

    assertEquals(expected, firstContentRow.get());
    assertEquals("Sheet1", firstContentRow.getHeader().getAttribute("worksheet"));

  }

  @Test
  public void testParseCorrectlyEmptyLeadingRowsAndColumns() throws IOException, InvalidFormatException, DataParserException {
    Workbook workbook = createWorkbook("/excel/TestExcelEmptyRowsCols.xlsx");

    WorkbookParser parser = new WorkbookParser(settingsWithHeader, getContext(), workbook, "Sheet1::0");

    // column header prefix, row value multiplier
    List<Pair<String, Integer>> sheetParameters = Arrays.asList(
            Pair.of("column", 1),
            Pair.of("header", 10)
    );

    for (int sheet = 1; sheet <= sheetParameters.size(); sheet++) {
      for (int row = 1; row <= 2; row++) {
        Record parsedRow = parser.parse();
        LinkedHashMap<String, Field> contentMap = new LinkedHashMap<>();
        String columnPrefix = sheetParameters.get(sheet - 1).getLeft();
        Integer valueMultiplier = sheetParameters.get(sheet - 1).getRight();
        for (int column = 1; column <= 3+sheet; column++) {
            contentMap.put(columnPrefix + column, Field.create(BigDecimal.valueOf(column * valueMultiplier)));
        }
        Field expectedRow = Field.createListMap(contentMap);
        assertEquals(String.format("Parsed value for sheet %1d, row %2d did not match expected value", sheet, row), expectedRow, parsedRow.get());
      }
    }
  }

  @Test
  public void testParseCorrectlyHandlesFileThatIgnoresHeaders() throws IOException, DataParserException, InvalidFormatException {
    Workbook workbook = createWorkbook("/excel/TestExcel.xlsx");

    WorkbookParser parser = new WorkbookParser(settingsIgnoreHeader, getContext(), workbook, "Sheet1::0");

    Record firstContentRow = parser.parse();

    LinkedHashMap<String, Field> contentMap = new LinkedHashMap<>();
    for (int i = 0; i <= 4; i++) {
      contentMap.put(String.valueOf(i), Field.create(new BigDecimal(i + 1)));
    }
    Field expected = Field.createListMap(contentMap);

    assertEquals(expected, firstContentRow.get());
  }

  @Test
  public void testParseCorrectlyHandlesFileWithNoHeaders() throws IOException, InvalidFormatException, DataParserException {
    Workbook workbook = createWorkbook("/excel/TestExcel.xlsx");

    WorkbookParser parser = new WorkbookParser(settingsNoHeader, getContext(), workbook, "Sheet1::0");

    Record firstContentRow = parser.parse();

    LinkedHashMap<String, Field> contentMap = new LinkedHashMap<>();
    for (int i = 0; i <= 4; i++) {
      contentMap.put(String.valueOf(i), Field.create("column" + (i + 1)));
    }
    Field expected = Field.createListMap(contentMap);

    assertEquals(expected, firstContentRow.get());
  }

  @Test
  public void testParseHandlesStartingFromANonZeroOffset() throws IOException, InvalidFormatException, DataParserException {
    InputStream file = getFile("/excel/TestOffset.xlsx");
    Workbook workbook = WorkbookFactory.create(file);
    WorkbookParserSettings settings = WorkbookParserSettings.builder()
        .withHeader(ExcelHeader.IGNORE_HEADER)
        .build();

    WorkbookParser parser = new WorkbookParser(settings, getContext(), workbook, "Sheet2::2");

    Record firstContentRow = parser.parse();

    LinkedHashMap<String, Field> contentMap = new LinkedHashMap<>();
    for (int i = 0; i <= 2; i++) {
      contentMap.put(String.valueOf(i), Field.create(new BigDecimal(i + 4)));
    }
    Field expected = Field.createListMap(contentMap);

    assertEquals(expected, firstContentRow.get());
  }

  @Test
  public void testParseHandlesMultipleSheets() throws IOException, InvalidFormatException, DataParserException {
    Workbook workbook = createWorkbook("/excel/TestMultipleSheets.xlsx");

    WorkbookParser parser = new WorkbookParser(settingsWithHeader, getContext(), workbook, "Sheet1::0");

    // column header prefix, row value multiplier
    List<Pair<String, Integer>> sheetParameters = Arrays.asList(
        Pair.of("column", 1),
        Pair.of("header", 10)
    );
    for (int sheet = 1; sheet <= sheetParameters.size(); sheet++) {
      for (int row = 1; row <= 2; row++) {
        Record parsedRow = parser.parse();
        LinkedHashMap<String, Field> contentMap = new LinkedHashMap<>();
        String columnPrefix = sheetParameters.get(sheet - 1).getLeft();
        Integer valueMultiplier = sheetParameters.get(sheet - 1).getRight();
        for (int column = 1; column <= 5; column++) {
          contentMap.put(columnPrefix + column, Field.create(BigDecimal.valueOf(column * valueMultiplier)));
        }
        Field expectedRow = Field.createListMap(contentMap);
        assertEquals(String.format("Parsed value for sheet %1d, row %2d did not match expected value", sheet, row), expectedRow, parsedRow.get());
      }
    }
  }

  @Test
  public void testParseHandlesBlanksCells() throws IOException, InvalidFormatException, DataParserException {
    Workbook workbook = createWorkbook("/excel/TestBlankCells.xlsx");

    WorkbookParser parser = new WorkbookParser(settingsWithHeader, getContext(), workbook, "Sheet1::0");

    Record recordFirstRow = parser.parse();

    LinkedHashMap<String, Field> firstContentMap = new LinkedHashMap<>();
    firstContentMap.put("column1", Field.create(BigDecimal.valueOf(11)));
    firstContentMap.put("column2", Field.create(""));
    firstContentMap.put("column3", Field.create(""));
    firstContentMap.put("column4", Field.create(BigDecimal.valueOf(44)));

    Field expectedFirstRow = Field.createListMap(firstContentMap);

    assertEquals(expectedFirstRow, recordFirstRow.get());
  }

  @Test
  public void testParseThrowsRecoverableDataExceptionForUnsupportedCellType() throws IOException, InvalidFormatException, DataParserException {
    Workbook workbook = createWorkbook("/excel/TestErrorCells.xlsx");
    WorkbookParser parser = new WorkbookParser(settingsWithHeader, getContext(), workbook, "Sheet1::0");

    exception.expect(RecoverableDataParserException.class);
    exception.expectMessage("EXCEL_PARSER_05 - Unsupported cell type ERROR");
    Record recordFirstRow = parser.parse();
  }
}