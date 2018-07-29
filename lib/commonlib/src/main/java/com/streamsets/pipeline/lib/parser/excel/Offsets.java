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
 * limitations under the License. See accompanying LICENSE file.
 */
package com.streamsets.pipeline.lib.parser.excel;

import com.streamsets.pipeline.lib.parser.DataParserException;
import org.apache.poi.ss.usermodel.Row;

import java.util.Optional;

public class Offsets {
  public static String offsetOf(Row row) {
    String sheetName = row.getSheet().getSheetName();
    int rowNum = row.getRowNum();
    return String.format("%s::%d", sheetName, rowNum);
  }

  public static Optional<Offset> parse(String offset) throws DataParserException {
    if (offset.equals("0")) {
      return Optional.empty();
    }
    String[] parts = offset.split("::");
    if (parts.length != 2) {
      throw new DataParserException(Errors.EXCEL_PARSER_06, offset);
    }
    String sheetName = parts[0];
    int rowNum = Integer.parseInt(parts[1]);
    return Optional.of(new Offset(sheetName, rowNum));
  }

  public static class Offset {
    private final String sheetName;
    private final int rowNum;

    public Offset(String sheetName, int rowNum) {
      this.sheetName = sheetName;
      this.rowNum = rowNum;
    }

    public String getSheetName() {
      return sheetName;
    }

    public int getRowNum() {
      return rowNum;
    }
  }
}
