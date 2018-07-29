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

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  EXCEL_PARSER_00("Workbooks are only supported as binary data"),
  EXCEL_PARSER_01("Error while reading workbook"),
  EXCEL_PARSER_02("Invalid document"),
  EXCEL_PARSER_03("Encrypted document"),
  EXCEL_PARSER_04("Empty workbook"),
  EXCEL_PARSER_05("Unsupported cell type {}"),
  EXCEL_PARSER_06("Error in offset format, expected SheetNumber::RowNumber, received {}"),
  ;

  private final String message;

  Errors(String message) {
    this.message = message;
  }

  @Override
  public String getCode() {
    return name();
  }

  @Override
  public String getMessage() {
    return message;
  }
}
