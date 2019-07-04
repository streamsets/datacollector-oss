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

import java.util.Collections;
import java.util.List;

public class WorkbookParserSettings {
  private ExcelHeader header;
  private List<String> sheets;
  private boolean skipCellsWithNoHeader;

  public ExcelHeader getHeader() {
    return header;
  }

  public void setHeader(ExcelHeader header) {
    this.header = header;
  }

  public List<String> getSheets() {
    return sheets;
  }

  public boolean shouldSkipCellsWithNoHeader() {
    return skipCellsWithNoHeader;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private ExcelHeader header;
    private List<String> sheets = Collections.emptyList();
    private boolean skipCellsWithNoHeader;

    private Builder() {
    }

    public Builder withHeader(ExcelHeader header) {
      this.header = header;
      return this;
    }

    public Builder withSheets(List<String> sheets) {
      this.sheets = sheets;
      return this;
    }

    public Builder withSkipCellsWithNoHeader(boolean skipCellsWithNoHeader) {
      this.skipCellsWithNoHeader = skipCellsWithNoHeader;
      return this;
    }

    public WorkbookParserSettings build() {
      WorkbookParserSettings workbookParserSettings = new WorkbookParserSettings();
      workbookParserSettings.header = this.header;
      workbookParserSettings.sheets = this.sheets;
      workbookParserSettings.skipCellsWithNoHeader = this.skipCellsWithNoHeader;
      return workbookParserSettings;
    }
  }
}
