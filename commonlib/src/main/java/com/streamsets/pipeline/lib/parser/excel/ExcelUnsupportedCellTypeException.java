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

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;

public class ExcelUnsupportedCellTypeException extends Exception {
  private CellType cellType;
  private Cell cell;

  public ExcelUnsupportedCellTypeException(final Cell cell, final CellType cellType) {
    super("Encountered unsupported cell type " + cellType.name());
    this.cell = cell;
    this.cellType = cellType;
  }

  public CellType getCellType() {
    return cellType;
  }

  public Cell getCell() {
    return cell;
  }
}
