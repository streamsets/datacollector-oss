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

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.ProtoConfigurableEntity.Context;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.ExcelHeader;
import com.streamsets.pipeline.lib.parser.AbstractDataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.RecoverableDataParserException;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class WorkbookParser extends AbstractDataParser {
  private static Logger LOG = LoggerFactory.getLogger(WorkbookParser.class);

  private final WorkbookParserSettings settings;
  private final Context context;
  private final Workbook workbook;
  private final ListIterator<Row> rowIterator;
  private String offset;
  private boolean eof;
  private FormulaEvaluator evaluator;
  private String currentSheet;

  private HashMap<String, List<Field>> headers;

  public WorkbookParser(WorkbookParserSettings settings,
                        Context context,
                        Workbook workbook,
                        String offsetId
  ) throws DataParserException {
    this.settings = requireNonNull(settings);
    this.context = requireNonNull(context);
    this.workbook = requireNonNull(workbook);
    this.rowIterator = iterate(this.workbook);
    this.offset = requireNonNull(offsetId);
    this.evaluator = workbook.getCreationHelper().createFormulaEvaluator();
    this.currentSheet = null; // default to blank.   Used to figure out when sheet changes and get new field names from header row

    if (!rowIterator.hasNext()) {
      throw new DataParserException(Errors.EXCEL_PARSER_04);
    }

    headers = new HashMap<>();

    // If Headers are expected, go through and get them from each sheet
    if (settings.getHeader() == ExcelHeader.WITH_HEADER) {
      Sheet sheet;
      String sheetName;
      Row hdrRow;
      for (int s=0; s<workbook.getNumberOfSheets(); s++) {
        sheet = workbook.getSheetAt(s);
        sheetName = sheet.getSheetName();

        if(!settings.getSheets().isEmpty() && !settings.getSheets().contains(sheetName)) {
          LOG.debug("Skipping sheet '{}'", sheetName);
          continue;
        }

        // In case that the given sheet is completely empty, we assume no headers, but continue processing
        if(!sheet.rowIterator().hasNext()) {
          headers.put(sheetName, Collections.emptyList());
          continue;
        }

        hdrRow = sheet.rowIterator().next();
        List<Field> sheetHeaders = new ArrayList<>();
        // if the table happens to have blank columns in front of it, loop through and artificially add those as headers
        // This helps in the matching of headers to data later as the indexes will line up properly.
        for (int columnNum=0; columnNum < hdrRow.getFirstCellNum(); columnNum++) {
          sheetHeaders.add(null);
        }
        for (int columnNum = hdrRow.getFirstCellNum(); columnNum < hdrRow.getLastCellNum(); columnNum++) {
          Cell cell = hdrRow.getCell(columnNum, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL);
          try {
            sheetHeaders.add(cell == null ? null : Cells.parseCell(cell, this.evaluator));
          } catch (ExcelUnsupportedCellTypeException e) {
            throw new DataParserException(Errors.EXCEL_PARSER_05, cell.getCellTypeEnum());
          }
        }
        headers.put(sheetName, sheetHeaders);
      }
    }

    Offsets.parse(offsetId).ifPresent(offset -> {
      String startSheetName = offset.getSheetName();
      int startRowNum = offset.getRowNum();

      while (rowIterator.hasNext()) {
        Row row = rowIterator.next();
        int rowNum = row.getRowNum();
        String sheetName = row.getSheet().getSheetName();
        // if a sheet has blank rows at the top then the starting row number may be higher than a default offset of zero or one, thus the >= compare
        if (startSheetName.equals(sheetName) && rowNum >= startRowNum) {
          if (rowIterator.hasPrevious()) {
            row = rowIterator.previous();
            this.currentSheet = row.getRowNum() == row.getSheet().getFirstRowNum() ? null : row.getSheet().getSheetName(); // used in comparison later to see if we've moved to new sheet
          }
          else {
            this.currentSheet = null;
          }
          break;
        }
      }
    });
  }

  private static ListIterator<Row> iterate(Workbook workbook) {
    return stream(workbook).flatMap(WorkbookParser::stream).collect(toList()).listIterator();
  }

  private static <T> Stream<T> stream(Iterable<T> it) {
    return StreamSupport.stream(it.spliterator(), false);
  }

  @Override
  public Record parse() throws DataParserException {
    if (!rowIterator.hasNext()) {
      eof = true;
      return null;
    }

    Row currentRow = rowIterator.next();

    // skip over rows that have cells but all cells are of BLANK celltype.
    while (shouldSkipRow(currentRow)) {
      if (rowIterator.hasNext()) {
        currentRow = rowIterator.next();
      }
      else {
        // end of file and this last row is blank.  Bail out.
        eof = true;
        return null;
      }
    }

    // see if a new worksheet has been entered.
    if (this.currentSheet == null || ! this.currentSheet.equals(currentRow.getSheet().getSheetName())) {
      this.currentSheet = currentRow.getSheet().getSheetName();
      // if header is expected, then jump over this row
      if (settings.getHeader() == ExcelHeader.WITH_HEADER || settings.getHeader() == ExcelHeader.IGNORE_HEADER) {
        if(rowIterator.hasNext()) {
          currentRow = rowIterator.next();  // move to the next row to parse as data
        } else {
          eof = true;
          return null;
        }
      }
    }


    offset = Offsets.offsetOf(currentRow);
    Record record = context.createRecord(offset);
    updateRecordWithCellValues(currentRow, record);
    return record;
  }

  @Override
  public String getOffset() {
    return eof ? "-1" : offset;
  }

  @Override
  public void close() throws IOException {
    workbook.close();
  }

  /**
   * Return true if the current row should be skipped for any reason.
   */
  private boolean shouldSkipRow(Row row) {
    // If we're running a mode that doesn't read all the sheets, skip all rows from the 'wrong' sheets
    if(!settings.getSheets().isEmpty()) {
      if(!settings.getSheets().contains(row.getSheet().getSheetName())) {
        return true;
      }
    }

    // Lastly skip all rows that are completely empty (BLANK cell type is everywhere)
    boolean isBlank = true;
    for (int columnNum = row.getFirstCellNum(); columnNum < row.getLastCellNum(); columnNum++) {
      Cell c = row.getCell(columnNum);
      isBlank = isBlank && (c == null || c.getCellTypeEnum() == CellType.BLANK);
      if (! isBlank) break;
    }
    return isBlank;
  }

  private void updateRecordWithCellValues(Row row, Record record) throws DataParserException {
    LinkedHashMap<String, Field> output = new LinkedHashMap<>();
    String sheetName = row.getSheet().getSheetName();
    String columnHeader;
    Set<String> unsupportedCellTypes = new HashSet<>();
    for (int columnNum = row.getFirstCellNum(); columnNum < row.getLastCellNum(); columnNum++) {
      if (headers.isEmpty()) {
        columnHeader = String.valueOf(columnNum);
      } else {
        if (columnNum >= headers.get(sheetName).size() || headers.get(sheetName).get(columnNum) == null) {
          // The current cell doesn't hae any associated header, which we conditionally skip
          if(settings.shouldSkipCellsWithNoHeader()) {
            continue;
          }

          columnHeader = String.valueOf(columnNum);
        } else {
          columnHeader = headers.get(sheetName).get(columnNum).getValueAsString();
        }
      }

      Cell cell = row.getCell(columnNum, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
      try {
        output.put(columnHeader, Cells.parseCell(cell, this.evaluator));
      } catch (ExcelUnsupportedCellTypeException e) {
        output.put(columnHeader, Cells.parseCellAsString(cell));
        unsupportedCellTypes.add(e.getCellType().name());
      }
    }


    // Set interesting metadata about the row
    Record.Header hdr = record.getHeader();
    hdr.setAttribute("worksheet", row.getSheet().getSheetName());
    hdr.setAttribute("row",  Integer.toString(row.getRowNum()));
    hdr.setAttribute("firstCol", Integer.toString(row.getFirstCellNum()));
    hdr.setAttribute("lastCol", Integer.toString(row.getLastCellNum()));
    record.set(Field.createListMap(output));
    if (unsupportedCellTypes.size() > 0) {
      throw new RecoverableDataParserException(record, Errors.EXCEL_PARSER_05, StringUtils.join(unsupportedCellTypes, ", "));
    }
  }
}
