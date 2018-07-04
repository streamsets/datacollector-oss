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
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.DateUtil;

import java.math.BigDecimal;

class Cells {
  static DataFormatter dataFormatter = new DataFormatter();

  static Field parseCell(Cell cell, FormulaEvaluator evaluator) throws ExcelUnsupportedCellTypeException {
    CellType cellType = cell.getCellTypeEnum();
//    set the cellType of a formula cell to its cached formula result type in order to process it as its result type
    if (cell.getCellTypeEnum().equals(CellType.FORMULA)) {
      cellType = cell.getCachedFormulaResultTypeEnum();
    }
    switch (cellType) {
      case STRING:
        return Field.create(cell.getStringCellValue());
      case NUMERIC:
        if (DateUtil.isCellDateFormatted(cell)) {
          // It's a date, not a number
          java.util.Date dt = cell.getDateCellValue();
          return Field.createDate(dt);
        }
        if (cell.getCellTypeEnum().equals(CellType.FORMULA)) {
          return Field.create(new BigDecimal(evaluator.evaluate(cell).formatAsString()));
        } else {
          return Field.create(new BigDecimal(dataFormatter.formatCellValue(cell)));
        }
      case BOOLEAN:
        return Field.create(cell.getBooleanCellValue());
      case BLANK:
        return Field.create("");
      default:
        throw new ExcelUnsupportedCellTypeException(cell, cellType);
    }
  }

  static Field parseCellAsString(Cell cell) {
    return Field.create(dataFormatter.formatCellValue(cell));
  }
}
