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
    // set the cellType of a formula cell to its cached formula result type in order to process it as its result type
    boolean isFormula = cell.getCellTypeEnum().equals(CellType.FORMULA);
    if (isFormula) {
      cellType = cell.getCachedFormulaResultTypeEnum();
    }

    switch (cellType) {
      case STRING:
        return Field.create(cell.getStringCellValue());
      case NUMERIC:
        Double rawValue = cell.getNumericCellValue();  // resolves formulas automatically and gets value without cell formatting
        String displayValue = isFormula ? evaluator.evaluate(cell).formatAsString() : dataFormatter.formatCellValue(cell);
        boolean numericallyEquivalent = false;
        try {
          numericallyEquivalent = Double.parseDouble(displayValue) == rawValue;
        } catch (NumberFormatException e) { }

        if (DateUtil.isCellDateFormatted(cell)) {
          // It's a date, not a number
          java.util.Date dt = cell.getDateCellValue();
          // if raw number is < 1 then it's a time component only, otherwise date.
          return rawValue < 1 ? Field.createTime(dt) : Field.createDate(dt);
        }

        // some machinations to handle integer values going in without decimal vs. with .0 for rawValue
        return Field.create(numericallyEquivalent ? new BigDecimal(displayValue) : BigDecimal.valueOf(rawValue));

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
