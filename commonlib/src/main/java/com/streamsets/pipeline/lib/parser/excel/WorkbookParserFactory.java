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

import com.streamsets.pipeline.config.ExcelHeader;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class WorkbookParserFactory extends DataParserFactory {
  public static final Map<String, Object> CONFIGS;
  public static final Set<Class<? extends Enum>> MODES;

  static {
    CONFIGS = new HashMap<>();
    CONFIGS.put(WorkbookParserConstants.SHEETS, Collections.emptyList());
    CONFIGS.put(WorkbookParserConstants.SKIP_CELLS_WITH_NO_HEADER, false);
    MODES = Collections.singleton(ExcelHeader.class);
  }

  public WorkbookParserFactory(Settings settings) {
    super(settings);
  }

  @Override
  public DataParser getParser(String id, InputStream is, String offset) throws DataParserException {
    return createParser(is, offset);
  }

  @NotNull
  private DataParser createParser(InputStream is, String offset) throws DataParserException {
    Workbook workbook = open(is);

    WorkbookParserSettings workbookSettings = WorkbookParserSettings.builder()
        .withSheets(getSettings().getConfig(WorkbookParserConstants.SHEETS))
        .withHeader(getSettings().getMode(ExcelHeader.class))
        .withSkipCellsWithNoHeader(getSettings().getConfig(WorkbookParserConstants.SKIP_CELLS_WITH_NO_HEADER))
        .build();
    return new WorkbookParser(workbookSettings, getSettings().getContext(), workbook, offset);
  }

  private Workbook open(InputStream is) throws DataParserException {
    try {
      return WorkbookFactory.create(is);
    } catch (IOException e) {
      throw new DataParserException(Errors.EXCEL_PARSER_01, e);
    } catch (InvalidFormatException e) {
      throw new DataParserException(Errors.EXCEL_PARSER_02, e);
    } catch (EncryptedDocumentException e) {
      throw new DataParserException(Errors.EXCEL_PARSER_03, e);
    }
  }

  @Override
  public DataParser getParser(String id, Reader reader, long offset) throws DataParserException {
    throw new DataParserException(Errors.EXCEL_PARSER_00);
  }
}
